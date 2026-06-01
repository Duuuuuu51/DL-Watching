# DL-Watching 技术实现需求文档

> Java 虚拟线程监控系统 — 设计规格说明书
>
> 版本: v1.0 | 日期: 2026-05-30 | 状态: Draft

---

## 目录

1. [总体架构](#1-总体架构)
2. [Layer 1: Java Agent 数据采集模块](#2-layer-1-java-agent-数据采集模块)
3. [Layer 2: 后端数据处理模块](#3-layer-2-后端数据处理模块)
4. [Layer 3: 数据存储模块](#4-layer-3-数据存储模块)
5. [告警引擎模块](#5-告警引擎模块)
6. [数据分析引擎模块](#6-数据分析引擎模块)
7. [智能体模块（三期预留）](#7-智能体模块三期预留)
8. [MVP 范围与版本路线](#8-mvp-范围与版本路线)

---

## 1. 总体架构

### 1.1 架构分层

```
┌──────────────────────────────────────────────────────────────┐
│                  前端可视化 (Grafana / 自研 Dashboard)          │
└──────────────────────────────────────────────────────────────┘
                               ↕ HTTP REST
┌──────────────────────────────────────────────────────────────┐
│  Layer 2: 数据处理层                                           │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────────────┐    │
│  │ 数据接收   │  │ 数据分析引擎   │  │ 告警引擎              │    │
│  │ gRPC/HTTP │  │ 阻塞根因分析   │  │ 统计异常检测 + 规则引擎 │    │
│  │ 校验/预处理 │  │ 趋势分析/慢任务│  │ 收敛/静默/升级        │    │
│  └─────┬────┘  └──────┬───────┘  └──────────┬───────────┘    │
│        └──────────────┼─────────────────────┘                │
│                       ↕ EventBus (内存队列, 接口抽象)          │
│  ┌──────────────────────────────────────────────────────┐    │
│  │          智能体模块 (MCP/RAG)  ← 三期预留               │    │
│  └──────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
         ↕ gRPC (批量上报, 双向流)       ↕ 写入
┌──────────────────────┐    ┌──────────────────────────────────┐
│  Layer 1: 数据采集层   │    │  Layer 3: 数据存储层               │
│  Java Agent           │    │  ClickHouse (明细/聚合查询)        │
│  · ASM 字节码增强      │    │  InfluxDB (时序指标/Grafana)      │
│  · 环形缓冲区          │    │  冷热分离 + ZSTD 压缩              │
│  · Protobuf 序列化     │    │  TTL 自动过期                     │
│  · 双向通信            │    │                                  │
└──────────────────────┘    └──────────────────────────────────┘
```

### 1.2 部署拓扑

```
被监控应用 1 (JVM + Agent.jar) ──┐
被监控应用 2 (JVM + Agent.jar) ──┼── gRPC ──→ 监控后端集群 ──→ ClickHouse
被监控应用 N (JVM + Agent.jar) ──┘            Spring Boot 3.2  ├─→ InfluxDB
                                              虚拟线程 GC        └─→ Grafana
```

### 1.3 技术栈选型

| 组件 | 选型 | 选型理由 |
|---|---|---|
| Java Agent 字节码增强 | ASM 9.7+ (核心Hook) + Byte Buddy (扩展Hook) | ASM 体积小、性能高、JDK内部类兼容好；Byte Buddy 用于应用框架Hook快速开发 |
| Agent-Backend 通信 | gRPC (Protobuf) + HTTP REST | gRPC 高性能批量上报；HTTP 供 Dashboard/配置管理 |
| 后端框架 | Spring Boot 3.2+ (虚拟线程支持) | 原生虚拟线程、生态成熟 |
| 序列化 | Protobuf (gRPC上报) + JSON (HTTP API) | 按场景选最优 |
| 时序存储 | ClickHouse (明细) + InfluxDB (指标) | ClickHouse 强于 OLAP 查询；InfluxDB 强于时序指标 + Grafana 集成 |
| 告警渠道 | Webhook 适配器模式 (钉钉/企微/邮件/Slack) | 插件化扩展 |
| 向量库 (三期) | Milvus / PGVector | RAG 检索 |

### 1.4 模块间通信关系

```
Agent ←──gRPC Bidirectional Stream──→ 数据接收网关
   ↕ Pull指令通过流的响应通道下发，Agent 不暴露端口

数据接收网关 ──EventBus──→ 存储写入器
                         → 告警评估器
                         → 分析引擎

Dashboard ←──HTTP REST──→ 后端查询API
```

---

## 2. Layer 1: Java Agent 数据采集模块

### 2.1 Hook 目标方法

#### 2.1.1 核心 Hook 点（ASM 实现）

| 序号 | 目标类 | Hook 方法 | 采集信息 | 实现要求 |
|---|---|---|---|---|
| 1 | `java.lang.VirtualThread` | 构造函数 `<init>` | 线程名、创建者线程ID、创建时间戳、创建栈帧(采样) | 在构造函数返回前插入采集逻辑 |
| 2 | `java.lang.VirtualThread` | `start()` | 启动时间戳 | 方法入口处采集 |
| 3 | `java.lang.VirtualThread` | `run()` | 首次调度时间、初始载体线程 | 方法入口处采集 |
| 4 | `java.lang.VirtualThread` | `mount()` (JDK内部) | 载体线程名、挂载时间戳 | 需反射访问，注意JDK版本兼容 |
| 5 | `java.lang.VirtualThread` | `unmount()` (JDK内部) | 卸载原因、挂载持续时间(us) | mount-unmount 配对计算 |
| 6 | `java.lang.VirtualThread` | `park()` / `parkNanos(long)` | Park原因（从调用栈推断）、开始时间戳 | 捕获参数+调用栈前3帧 |
| 7 | `java.lang.VirtualThread` | `unpark()` | Unpark来源、Park持续时长 | 与 park 配对，计算阻塞时长 |
| 8 | `jdk.internal.misc.VirtualThreadScheduler` | `execute(Runnable)` | 调度延迟、队列深度 | 需 Hook JDK 内部类 |
| 9 | `java.lang.VirtualThread` | `terminate()` (内部) | 总生命周期、终态状态快照 | 终态事件，包含线程全生命周期汇总 |

#### 2.1.2 扩展 Hook 点（Byte Buddy 实现）

| 序号 | 目标 | 说明 |
|---|---|---|
| 10 | `java.util.concurrent.ThreadPoolExecutor` | 监控平台线程池状态，关联载体线程来源 |
| 11 | Tomcat/Jetty/Netty 请求处理入口 | 将 HTTP 请求与虚拟线程关联 |
| 12 | 用户自定义包（通过 Agent 配置指定） | 支持用户指定额外需要监控的包/类 |

**验收标准：**
- [ ] 9 个核心 Hook 点全部实现，覆盖率 100%
- [ ] Hook 代码总性能开销 < 3%（以 SPECjvm 基准）
- [ ] 支持 JDK 21/22/23/24（每版本差异通过特性检测自动适配）
- [ ] 扩展 Hook 点默认不启用，通过配置开启

### 2.2 字节码增强实现

#### 2.2.1 方案

使用 **ASM ClassVisitor + ClassFileTransformer** 组合：

```
JVM加载类 → ClassFileTransformer.transform() → ASM ClassVisitor 判断目标类
                                                      ↓
                                              匹配目标类? ──是──→ 插入采集字节码
                                                      ↓否
                                                    跳过
```

| 技术点 | 实现要求 |
|---|---|
| ASM 版本 | ASM 9.7+，直接操作字节码，无反射开销 |
| 类加载拦截 | `java.lang.instrument.ClassFileTransformer`，premain 模式注册 |
| JDK 内部类访问 | 使用 `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED` |
| 特性检测 | Agent 启动时检测 JDK 版本 → 适配不同版本的内部 API 变化 |
| 安全策略 | 只 Hook 指定类，不修改 java.* 外的核心类库，避免兼容性风险 |

**验收标准：**
- [ ] Agent jar 大小 < 1MB（含 ASM + Protobuf runtime）
- [ ] Agent 启动时间增量 < 500ms
- [ ] 在 JDK 21/22/23/24 上通过兼容性测试

### 2.3 本地缓存与批量上报

#### 2.3.1 缓存设计

```
[事件产生] → ConcurrentLinkedDeque<Event> (无锁写入)
                  ↓ 容量触发 / 定时触发
            [批量聚合器: 500条/批 或 3s间隔]
                  ↓
            [Protobuf 序列化]
                  ↓
            [gRPC Stream 上报]
```

| 参数 | 默认值 | 可配置范围 | 说明 |
|---|---|---|---|
| 环形缓冲区容量 | 10,000 条 | 1,000-100,000 | 超出丢弃最旧 |
| 内存上限 | 64 MB | 16-256 MB | 防止 Agent OOM |
| 批量大小 | 500 条/批 | 100-1,000 | 数量触发 |
| 上报间隔 | 3 秒 | 1-30 秒 | 时间兜底触发 |
| 事件过期 | 60 秒 | 30-300 秒 | 超时未上报丢弃 |
| 序列化格式 | **Protobuf** | / | 比 JSON 体积减少 60-70% |

#### 2.3.2 重试与容错

| 场景 | 策略 |
|---|---|
| 上报失败 | 指数退避重试：1s → 2s → 4s → 8s，最多 3 次 |
| 3 次重试均失败 | 丢弃该批次，计数器 +1（通过心跳上报失败计数） |
| 连接断开 | gRPC 自动重连，重连期间事件继续缓存 |
| 缓存溢出 | 丢弃最旧事件（FIFO 环形缓冲区满时覆盖），记录溢出次数 |

**验收标准：**
- [ ] Agent 内存额外占用 < 64MB（默认配置下）
- [ ] 批量上报 CPU 占用 < 2%（单核）
- [ ] 网络断开 30s 内数据不丢失（缓存容量内）
- [ ] 重连成功后自动恢复上报，无数据重复（batch_seq 去重）

### 2.4 双向通信设计

#### 2.4.1 通信模式

```
Push 模式（常态）: Agent ──gRPC Bidirectional Stream──→ Backend
  · 批量事件上报
  · 心跳 + 应用元信息

Pull 模式（按需）: Backend ──在 Report 流的响应通道下发指令──→ Agent
  · 查询指定虚拟线程当前状态
  · 触发虚拟线程级 Dump
  · 临时调整采样率
  · 配置热更新
```

**关键设计决策：** Agent **不暴露任何网络端口**，所有 Pull 指令通过已建立的 gRPC 双向流的响应通道下发。原因：(1) 企业网络通常不允许 Backend 直连 Agent；(2) 减少 Agent 攻击面；(3) 连接管理简化。

#### 2.4.2 Agent 生命周期

```
Agent启动 → Register(认证 + 应用元信息) → 获取 SessionToken + 配置
         → Report(bidirectional stream) ←→ 持续双向通信
         → Heartbeat(每10s)              维持在线状态
         → JVM关闭 → Report流关闭 → Backend标记实例下线
```

**验收标准：**
- [ ] Agent 启动后 5s 内完成注册并建立 Report 流
- [ ] Pull 指令响应延迟 P99 < 5s
- [ ] Agent 意外退出时 Backend 在 30s 内（心跳超时）感知并标记下线

### 2.5 配置管理

#### 2.5.1 本地配置（agent.properties）

```properties
# 必须配置
dlwatching.app.id=order-service
dlwatching.backend.host=monitor.example.com
dlwatching.backend.port=9090
dlwatching.auth.token=${DLW_TOKEN}

# 可选配置（有默认值）
dlwatching.batch.size=500
dlwatching.batch.interval.ms=3000
dlwatching.cache.max.events=10000
dlwatching.cache.max.memory.mb=64
dlwatching.sample.rate=0.05
dlwatching.log.level=INFO
```

#### 2.5.2 远程配置（Backend 下发）

支持通过 gRPC ControlCommand 动态更新：
- `batch_size`, `flush_interval_ms` — 运行时调整上报策略
- `sample_rate` — 调节栈帧采样率（0.0 ~ 1.0）
- `hook_enabled` — 紧急情况下关闭部分 Hook
- `log_level` — 动态调整 Agent 日志级别

**验收标准：**
- [ ] 远程配置变更后 10s 内在 Agent 生效
- [ ] 本地配置优先级高于远程默认配置（防止远程错误配置导致 Agent 不可用）

---

## 3. Layer 2: 后端数据处理模块

### 3.1 gRPC 接收网关

#### 3.1.1 Proto 定义

```protobuf
service VirtualThreadMonitor {
  // Agent注册
  rpc Register(RegisterRequest) returns (RegisterResponse);

  // 双向流：批量事件上报 + 控制指令下发
  rpc Report(stream EventBatch) returns (stream ControlCommand);

  // 心跳保活
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}

message RegisterRequest {
  string app_id = 1;
  string instance_id = 2;     // hostname_pid
  string jdk_version = 3;
  string agent_version = 4;
  string auth_token = 5;
}

message RegisterResponse {
  string session_token = 1;
  AgentConfig config = 2;
}

message EventBatch {
  string app_id = 1;
  string instance_id = 2;
  int64 batch_seq = 3;
  int64 timestamp_ms = 4;
  repeated ThreadEvent events = 5;
}

message ThreadEvent {
  EventType type = 1;
  int64 thread_id = 2;
  string thread_name = 3;
  int64 timestamp_ms = 4;
  string carrier_thread = 5;
  int64 duration_us = 6;
  string reason = 7;
  StackFrame caller = 8;

  enum EventType {
    CREATED = 0; STARTED = 1;
    MOUNTED = 2; UNMOUNTED = 3;
    PARKED = 4; UNPARKED = 5;
    TERMINATED = 6; HEARTBEAT = 7;
  }
}

message ControlCommand {
  CommandType type = 1;
  string command_id = 2;
  oneof payload {
    AgentConfig new_config = 3;
    ThreadQueryParams query_params = 4;
  }

  enum CommandType {
    ACK = 0; SLOW_DOWN = 1; SPEED_UP = 2;
    QUERY_THREAD = 3; DUMP_THREADS = 4; UPDATE_CONFIG = 5;
  }
}
```

#### 3.1.2 安全认证

| 方式 | 说明 |
|---|---|
| **Token 认证** | Agent 配置中预设 App Token，Register 时换取 Session Token（24h 有效期） |
| **Token 续期** | 心跳自动续期，Session Token 失效前 1h 自动刷新 |
| **校验链** | gRPC interceptor → 提取 metadata `authorization: Bearer <session_token>` → 验证有效性 |
| **隔离** | 应用 A 的 token 只能上报 app_id=A 的数据，跨应用操作拒绝 |

**验收标准：**
- [ ] 未认证请求 100% 拒绝
- [ ] Token 过期自动续期无感知
- [ ] 跨应用伪造数据 100% 拦截

### 3.2 数据校验规则

#### 3.2.1 分层校验

```
第一层 ─ 协议层 (gRPC interceptor)
  ├── Protobuf 反序列化校验（字段类型、必填项）
  ├── Authorization header 有效性
  └── 请求体 ≤ 2MB

第二层 ─ 业务语义层 (事件处理器)
  ├── app_id 白名单校验
  ├── instance_id 与 token 绑定的 app_id 一致性
  ├── timestamp_ms 单调性（不早于上次 30s+）
  ├── batch_seq 连续性（gap > 1000 → 告警）
  └── 事件状态机合法性（如: 同一 thread_id 不能连续两次 PARKED 无 UNPARKED）

第三层 ─ 数据质量层 (清洗管线)
  ├── duration_us ∈ [0, 86400000000] (0~24h)
  ├── thread_id > 0
  └── carrier_thread 格式合法性（非空合理线程名）
```

#### 3.2.2 校验失败处理

| 层级 | 处理方式 |
|---|---|
| 协议层失败 | 直接拒绝请求，返回 gRPC 错误码，计数 → 错误率过高触发熔断 |
| 语义层失败 | 拒绝整批次，返回 ControlCommand (ERROR + 错误描述) |
| 质量层失败 | 接受数据，标记 `quality=low`，不入告警评估，仅存档 |

**验收标准：**
- [ ] 三层校验全部实现
- [ ] 单批次校验耗时 < 1ms
- [ ] 校验失败事件 100% 记录到审计日志

### 3.3 限流熔断策略

```
┌────────────────────────────────────────────────────┐
│ gRPC 网关层                                          │
│ · 单实例 Report 流上限: 1 条（重复连接踢旧）           │
│ · 全局并发 Stream: 10,000                           │
│ · 连接建立速率: 100/s（防注册风暴）                    │
├────────────────────────────────────────────────────┤
│ 业务层 (令牌桶)                                       │
│ · 实例级: 5,000 事件/s, 突发 10,000                  │
│ · 全局级: 100 万事件/s (可配置)                       │
├────────────────────────────────────────────────────┤
│ 熔断器 (实例维度, 1min 滑动窗口)                       │
│ · OPEN 条件: 错误率 > 50%                           │
│ · OPEN 持续: 60s                                    │
│ · HALF_OPEN: 连续 3 次成功 → CLOSE                   │
├────────────────────────────────────────────────────┤
│ 降级 (存储端压力触发)                                  │
│ · EventBus 队列积压 > 1000万 → 采样模式(仅10%)        │
│ · ClickHouse 写入延迟 > 10s → 暂停非关键事件写入       │
│   关键事件: ERROR、长阻塞(>1s)、线程创建/终止           │
│   非关键: MOUNT/UNMOUNT 正常调度事件                   │
└────────────────────────────────────────────────────┘
```

**验收标准：**
- [ ] 限流触发后 Agent 收到 SLOW_DOWN 指令并降速
- [ ] 熔断恢复后 30s 内数据上报恢复正常
- [ ] 降级期间关键事件不丢失

### 3.4 数据预处理管线

```
原始事件 → [清洗] → [转换] → [丰富] → [聚合] → [路由分发]
```

#### 3.4.1 清洗

| 操作 | 规则 | 说明 |
|---|---|---|
| 去重 | `app_id + instance_id + batch_seq + event_hash`，5min 窗口幂等 | 基于 Redis BitMap |
| 异常值过滤 | duration_us ∈ [0, 86400000000] | 超范围丢弃 |
| 空字段填充 | thread_name 为空 → `"vt-{thread_id}"` | 保证查询友好 |
| 状态修正 | 同一 thread_id 连续两个 CREATED → 保留首个 | JDK 内部场景 |

#### 3.4.2 转换

| 操作 | 说明 |
|---|---|
| 时间戳统一 | Agent 时间戳保留为 `client_ts`，服务端接收时间为 `server_ts` |
| 枚举映射 | Protobuf enum → 小写字符串存储 |
| 栈帧压缩 | `caller` 全量仅保留 5% 采样，其余只保留 `className.methodName` |

#### 3.4.3 丰富

| 操作 | 说明 |
|---|---|
| 应用元信息补充 | app_id → 应用名、环境(prod/staging)、所属团队 |
| 载体线程池标注 | `ForkJoinPool-1-worker-5` → 提取池名 `ForkJoinPool-1` |

#### 3.4.4 预聚合（窗口级）

| 维度 | 窗口 | 输出指标 |
|---|---|---|
| `app_id + event_type` | 1min 滚动 | count, avg_dur, p50, p99, max_dur |
| `app_id + reason` | 5min 滚动 | park 原因 TopN 分布 |
| `carrier_pool` | 1min 滚动 | mount_count, avg_mount_dur |

#### 3.4.5 路由分发

| 目标 | 数据类型 |
|---|---|
| ClickHouse | 原始明细 + 窗口聚合 |
| InfluxDB | 聚合时序指标 |
| 告警引擎 | 实时事件流（EventBus 推送） |
| 死信队列 | 校验失败但不明确的数据（文件持久化） |

**验收标准：**
- [ ] 端到端延迟（Agent发送 → ClickHouse可查）P99 < 10s
- [ ] 去重准确率 100%
- [ ] 预聚合结果与原始数据一致（误差 < 0.1%）

### 3.5 HTTP REST API（管理 + Dashboard）

| 方法 | 端点 | 用途 |
|---|---|---|
| GET | `/api/v1/health` | 健康检查 |
| GET | `/api/v1/apps` | 应用列表 |
| GET | `/api/v1/apps/{appId}/instances` | 实例列表与在线状态 |
| GET | `/api/v1/threads/{threadId}` | 指定虚拟线程详情 |
| POST | `/api/v1/threads/search` | 多条件搜索 (`appId`, `timeRange`, `threadName`, `reason`, `durationRange`) |
| GET | `/api/v1/analytics/blocking?appId=&from=&to=` | 阻塞根因分析报告 |
| GET | `/api/v1/analytics/trends?appId=&metric=&from=&to=` | 性能趋势数据 |
| GET | `/api/v1/analytics/slow-tasks?appId=&threshold=&from=&to=` | 慢任务列表 |
| GET | `/api/v1/alerts/rules` | 告警规则列表 |
| POST/PUT/DELETE | `/api/v1/alerts/rules[/{id}]` | 告警规则 CRUD |
| POST | `/api/v1/config/agent/{instanceId}` | 下发 Agent 配置 |

---

## 4. Layer 3: 数据存储模块

### 4.1 ClickHouse 表设计

#### 4.1.1 原始事件明细表

```sql
CREATE TABLE vt_events (
    app_id          LowCardinality(String),
    instance_id     String,
    event_type      Enum8(
        'CREATED' = 0, 'STARTED' = 1,
        'MOUNTED' = 2, 'UNMOUNTED' = 3,
        'PARKED' = 4, 'UNPARKED' = 5,
        'TERMINATED' = 6
    ),
    thread_id       Int64,
    thread_name     String,
    carrier_thread  String,
    duration_us     Int64,
    reason          String,
    caller_class    String,
    caller_method   String,
    quality         Enum8('normal' = 0, 'low' = 1),  -- 数据质量标记
    client_ts       DateTime64(3),
    server_ts       DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(server_ts)
ORDER BY (app_id, event_type, server_ts)
TTL server_ts + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192,
         compression_codec = 'ZSTD(3)';
```

**索引设计：**

```sql
-- 加速按线程ID查询
ALTER TABLE vt_events ADD INDEX idx_thread_id thread_id TYPE bloom_filter GRANULARITY 4;

-- 加速按原因查询
ALTER TABLE vt_events ADD INDEX idx_reason reason TYPE bloom_filter GRANULARITY 4;
```

#### 4.1.2 分钟级聚合表

```sql
CREATE TABLE vt_metrics_1min (
    app_id        LowCardinality(String),
    event_type    String,
    window_ts     DateTime,
    count         UInt32,
    avg_duration  Float64,
    p50_duration  Float64,
    p99_duration  Float64,
    max_duration  Int64
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_ts)
ORDER BY (app_id, event_type, window_ts)
TTL window_ts + INTERVAL 90 DAY DELETE
SETTINGS compression_codec = 'ZSTD(6)';
```

**验收标准：**
- [ ] 明细表写入吞吐 ≥ 50 万行/s（单节点）
- [ ] 聚合表查询 P99 < 500ms（Dashboard 常用查询）
- [ ] TTL 自动清理正常运行

### 4.2 InfluxDB 设计

#### 4.2.1 Measurement 与 Schema

| Measurement | Tags | Fields | 保留策略 |
|---|---|---|---|
| `vt_throughput` | `app_id`, `instance_id`, `event_type` | `count` | 30d |
| `vt_duration` | `app_id`, `event_type` | `avg`, `p50`, `p95`, `p99`, `max` | 90d |
| `vt_active_count` | `app_id`, `instance_id` | `mounted`, `parked`, `runnable` | 30d |
| `vt_scheduler` | `app_id`, `instance_id` | `queue_depth`, `carrier_pool_size`, `active_carriers` | 30d |
| `vt_error_rate` | `app_id`, `instance_id`, `error_type` | `count`, `rate_per_min` | 90d |

**验收标准：**
- [ ] Grafana Dashboard 配置完成，覆盖以上 5 个 Measurement
- [ ] InfluxDB 写入 P99 < 100ms

### 4.3 冷热数据分离

| 数据级别 | 时间范围 | 存储介质 | 压缩 | 说明 |
|---|---|---|---|---|
| **热** | 0-3 天 | SSD | 无 / 轻量 | ClickHouse 明细高频查询 |
| **温** | 3-30 天 | SSD + HDD 混合 | ZSTD(3) | ClickHouse 明细归档 |
| **冷** | 30-90 天 | HDD | ZSTD(9) | 仅保留聚合数据 |
| **归档** | 90 天+ | 对象存储 (MinIO/S3) | ZSTD(9) | parquet 格式，按需恢复查询 |

**实现方式：**
- ClickHouse `TTL ... TO VOLUME` 语句自动迁移
- InfluxDB Retention Policy 自动过期

**验收标准：**
- [ ] 热数据 100% 在 SSD 上
- [ ] 数据迁移过程对查询无影响（在线迁移）
- [ ] 归档数据恢复至 ClickHouse 可查询 < 5min（10GB 以内）

---

## 5. 告警引擎模块

### 5.1 统计异常检测（默认，开箱即用）

不依赖用户配置阈值，Agent 上报数据积累 7 天后自动建立基线。

| 检测方法 | 算法 | 触发条件 | 适用场景 |
|---|---|---|---|
| **移动平均偏离** | MA(7天同时段) ± 3σ | 当前值超出 3 倍标准差 | 通用异常检测 |
| **突增检测** | 环比增长率 | (now_1min - prev_5min) / prev_5min > 200% | 突发故障 |
| **趋势上升** | 线性回归 | 斜率 > 阈值，持续 ≥ 10 个数据点 (10min) | 慢泄露 |
| **零值检测** | 直接判断 | `vt_active_count.mounted == 0` 持续 2min | 应用假死 |
| **长阻塞检测** | 直接判断 | 单线程 `duration_us > 60000000` (60s) | 严重阻塞 |

**基线建立过程：**
```
接入后第 1-7 天: 仅记录，不发告警（学习期）
第 8 天起: 基于 7 天历史数据计算基线 → 开启异常检测
基线每日更新: 滚动窗口，包含最近 7 天数据
```

**验收标准：**
- [ ] 接入 7 天后自动开启异常检测，无需人工配置
- [ ] 误报率 < 10%（以运维人工确认统计）
- [ ] 漏报率 < 5%（重大故障 100% 触发告警）

### 5.2 规则引擎（扩展能力）

用户可自定义规则，覆盖统计检测无法处理的场景：

```yaml
# 规则配置示例
rules:
  - name: "虚拟线程池饥饿"
    description: "大量虚拟线程因无可用载体线程而挂起"
    metric: vt_scheduler.queue_depth
    condition: "> 100"
    duration: 5m
    severity: P2
    app_ids: ["*"]

  - name: "单线程挂载时间异常长"
    description: "单虚拟线程占用载体线程超30秒"
    metric: single_vt.mount_duration
    condition: "> 30000"
    severity: P3

  - name: "应用完全失联"
    description: "Agent心跳超时"
    metric: heartbeat
    condition: "absent"
    duration: 2m
    severity: P1
    channels: ["dingtalk", "wecom"]  # 覆盖默认渠道
```

**规则引擎架构：**
```
规则定义 (YAML/DB) → 规则解析器 → 条件求值引擎 (CQL-like)
                                        ↓
                              EventBus 事件流 → 条件匹配 → 触发告警
```

**验收标准：**
- [ ] 规则变更后 30s 内生效（热加载）
- [ ] 支持 AND/OR 复合条件
- [ ] 单规则评估延迟 < 100ms

### 5.3 告警生命周期管理

```
[触发] → [收敛检查] → [静默检查] → [发送] → [等待确认] → [升级/恢复]
                ↓             ↓                      ↓
          5min内重复?    静默窗口内?           30min未确认?
           → 抑制          → 降级发送            → 升级 P1
```

| 机制 | 规则 | 配置方式 |
|---|---|---|
| **收敛** | 同一 `(app_id, rule_name)` 5min 内只发 1 次 | 规则级配置 |
| **静默窗口** | 支持按时间段（如 `02:00-05:00`）、按应用（发版中） | 全局 + 应用级 |
| **升级** | P2 → 30min 未确认 → P1；P1 → 15min 未确认 → 电话通知 | 规则级配置 |
| **聚合摘要** | 5min 窗口内所有告警 → 汇总为一条 Markdown 消息 | 渠道级配置 |

**验收标准：**
- [ ] 收敛后同告警 5min 内不重复发送
- [ ] 静默窗口内告警被正确抑制
- [ ] 升级链路完整触发

### 5.4 告警渠道

**适配器接口（插件化）：**

```java
public interface AlertChannel {
    /**
     * @return 渠道标识，如 "dingtalk", "email", "wecom"
     */
    String channelId();

    /**
     * 发送告警消息
     * @param message 告警消息体
     * @return 发送是否成功
     */
    boolean send(AlertMessage message);

    /**
     * 渠道健康检查
     */
    boolean isHealthy();

    /**
     * 渠道支持的消息格式
     */
    Set<MessageFormat> supportedFormats(); // MARKDOWN, TEXT, HTML
}
```

| 渠道 | 实现 | 消息格式 | 适用级别 |
|---|---|---|---|
| **钉钉** | Webhook + 钉钉机器人 Markdown | ActionCard / Markdown | P1, P2 |
| **企业微信** | Webhook + 企微机器人 | Markdown | P1, P2 |
| **邮件** | SMTP + Thymeleaf HTML 模板 | HTML | P2, P3, 日报 |
| **通用 Webhook** | HTTP POST JSON | JSON (标准格式) | 自定义对接飞书/Slack/自研平台 |

**验收标准：**
- [ ] 新增渠道只需实现 AlertChannel 接口 + 配置文件注册（无需改核心代码）
- [ ] 渠道故障不影响其他渠道发送
- [ ] 渠道发送失败有日志记录 + 重试（最多 2 次）

---

## 6. 数据分析引擎模块

### 6.1 阻塞根因分析

#### 6.1.1 算法流程

```
输入: app_id, time_range (开始时间, 结束时间)

Step 1 ─ TopN 阻塞原因聚类
  SELECT reason, COUNT(*) as cnt, SUM(duration_us) as total_dur
  FROM vt_events
  WHERE app_id = ? AND event_type = 'PARKED'
    AND server_ts BETWEEN ? AND ?
  GROUP BY reason
  ORDER BY total_dur DESC
  LIMIT 5

Step 2 ─ 载体线程饥饿检测
  FOR EACH Top reason:
    SELECT carrier_thread, COUNT(DISTINCT thread_id) as vt_count
    FROM vt_events
    WHERE app_id = ? AND reason = ?
      AND server_ts BETWEEN ? AND ?
    GROUP BY carrier_thread
    HAVING vt_count > (SELECT baseline_avg * 2 FROM baseline)
    → IF found: 标记为"调度饥饿 - carrier线程过载"

Step 3 ─ 业务代码热点识别
  SELECT caller_class, caller_method, COUNT(*) as cnt
  FROM vt_events
  WHERE app_id = ? AND event_type = 'PARKED'
    AND server_ts BETWEEN ? AND ?
    AND caller_class != ''  -- 排除无调用栈的事件
  GROUP BY caller_class, caller_method
  ORDER BY cnt DESC
  LIMIT 10
  → IF 某调用链阻塞比例 > 30%: 标记为"业务代码热点"

Step 4 ─ 聚合并输出根因报告
```

#### 6.1.2 输出格式

```json
{
  "app_id": "order-service",
  "time_range": {"from": "...", "to": "..."},
  "primary_cause": "ReentrantLock竞争",
  "confidence": 0.87,
  "top_blocking_reasons": [
    {"reason": "j.u.c.l.ReentrantLock$Sync.lock", "total_duration_s": 450, "percentage": 62},
    {"reason": "External IO - HTTP call", "total_duration_s": 180, "percentage": 25},
    {"reason": "VirtualThread.park (sleep)", "total_duration_s": 95, "percentage": 13}
  ],
  "hotspot_methods": [
    {"class": "com.example.OrderService", "method": "processOrder", "block_count": 1200, "avg_dur_ms": 375}
  ],
  "scheduler_starvation": true,
  "starving_carrier_threads": ["ForkJoinPool-1-worker-3"],
  "suggested_actions": [
    "将 OrderService.processOrder 中的 synchronized 块替换为 ReentrantLock + tryLock(timeout)",
    "考虑扩大 ForkJoinPool 并行度（当前: 8, 建议: 16）"
  ]
}
```

**验收标准：**
- [ ] 根因分析耗时 < 3s（监控 50 应用、30 天数据量）
- [ ] Top 阻塞原因与实际根因一致率 > 80%
- [ ] suggested_actions 可操作性强（非泛化建议）

### 6.2 性能趋势分析

| 指标 | 计算方式 | 对比基线 |
|---|---|---|
| **虚拟线程创建速率** | COUNT(CREATED) / min | vs 前一日同时段 ±30% 告警 |
| **Park 时长趋势** | AVG(duration_us) 滑动窗口(5min) | vs 前 7 日均值 |
| **调度延迟** | (unmount_ts - schedule_ts) P99 | vs 前一日 |
| **线程泄漏检测** | active_threads 线性回归 | 斜率 > 0 持续 1h → 疑似泄漏 |

**验收标准：**
- [ ] 趋势数据查询 P99 < 1s
- [ ] 线程泄漏检测准确率 > 90%

### 6.3 慢任务检测

**判定规则（满足任一即标记）：**

1. **统计异常慢**: `duration_us > P99 * 3`（同应用同时段）
2. **用户配置阈值**: `duration_us > 用户配置的阈值_ms`
3. **长时间阻塞**: 状态为 PARKED 且 `duration > 60s`

**输出信息：**
- 虚拟线程 ID + 名称
- 阻塞原因 + 调用栈
- 阻塞开始时间 + 持续时长
- 载体线程 + 所在实例

**验收标准：**
- [ ] 慢任务检测延迟 < 30s（事件到达 → 标记为慢任务）
- [ ] P99 基线每日自动更新

---

## 7. 智能体模块（三期预留）

### 7.1 设计目标

提供自然语言对话窗口，用户描述问题 → 大模型自主选择 MCP 工具 → RAG 检索相关日志/事件 → 给出诊断分析。

### 7.2 架构

```
用户自然语言提问
      ↓
  LLM (理解意图 + 选择工具)
      ↓
  MCP Server (工具调度层)
      ├→ Tool: query_thread_errors    → ClickHouse 查询
      ├→ Tool: get_thread_dump        → Agent Pull 指令
      ├→ Tool: get_blocking_analysis  → 分析引擎 API
      └→ Tool: search_similar_incidents → RAG 检索 (向量库)
             ↓
         收集各工具返回结果 → LLM 综合分析 → 自然语言回答
```

### 7.3 MCP 工具定义

```json
[
  {
    "name": "query_thread_errors",
    "description": "查询应用在指定时间范围内的虚拟线程异常/错误事件分布",
    "inputSchema": {
      "type": "object",
      "properties": {
        "app_id": {"type": "string", "description": "应用标识"},
        "time_range": {"type": "string", "description": "如 'last_30m', '2026-05-30T10:00/2026-05-30T11:00'"},
        "error_type": {"type": "string", "description": "错误类型过滤，可选"}
      },
      "required": ["app_id", "time_range"]
    }
  },
  {
    "name": "get_thread_dump",
    "description": "获取指定虚拟线程的当前栈帧快照（通过Agent Pull指令实时采集）",
    "inputSchema": {
      "type": "object",
      "properties": {
        "app_id": {"type": "string"},
        "thread_id": {"type": "integer"},
        "instance_id": {"type": "string"}
      },
      "required": ["app_id", "thread_id"]
    }
  },
  {
    "name": "get_blocking_analysis",
    "description": "获取阻塞根因分析报告，包含Top阻塞原因、热点方法、调度饥饿检测",
    "inputSchema": {
      "type": "object",
      "properties": {
        "app_id": {"type": "string"},
        "time_range": {"type": "string"}
      },
      "required": ["app_id", "time_range"]
    }
  },
  {
    "name": "search_similar_incidents",
    "description": "通过RAG检索历史上相似的异常事件模式，用于模式对比",
    "inputSchema": {
      "type": "object",
      "properties": {
        "app_id": {"type": "string"},
        "error_signature": {"type": "string", "description": "错误特征描述"},
        "time_range": {"type": "string", "description": "检索的时间范围，默认最近30天"}
      },
      "required": ["app_id", "error_signature"]
    }
  }
]
```

### 7.4 RAG 设计

#### 7.4.1 索引构建

```
异常事件窗口(5min) → 文本化签名 → Embedding模型 → 向量库
```

**文本化签名格式：**
```
[{app_id}] {时间段} | 事件总数:{n} | Top阻塞原因:{reason1}({pct}%),{reason2}({pct}%)
| P99延迟:{dur}us | 错误率:{rate}% | 载体线程池饱和度:{sat}%
```

**Embedding 模型：** BGE-M3（中文友好，1024维，开源可本地部署）

**向量库：** Milvus（生产）或 PGVector（轻量部署）

#### 7.4.2 检索流程

```
1. 用户提问 → LLM 提取关键特征（app_id, error_type, time_range, symptom）
2. 特征文本 → Embedding → 向量检索(TopK=5)
3. 检索结果 → rerank(Cross-Encoder) → Top 3 相似案例
4. Top 3 + 当前实时数据 → LLM → 综合分析报告
```

**验收标准（三期评估）：**
- [ ] MCP 工具调用准确率 > 90%（LLM 选对工具）
- [ ] RAG 检索命中率 > 70%（Top 5 中包含实际相似案例）
- [ ] 端到端响应 < 30s（用户提问 → 分析报告）

---

## 8. MVP 范围与版本路线

### 8.1 v0.5 MVP — 核心数据链路（数据采集 → 接收 → 存储，打通可演示）

**范围:** Agent 数据采集 + 后端数据接收 + 数据存储 + Grafana 基础看板

| 模块 | MVP 交付内容 | 关键验收 |
|---|---|---|
| **Java Agent** | 9 个核心 Hook(ASM) + Protobuf批量上报 + gRPC双向流 + 环形缓冲区(10000条) | Agent 可独立部署到任意 JDK 21+ 应用，上报数据到后端 |
| **后端接收** | gRPC网关(Register/Report/Heartbeat) + 三层校验 + 限流熔断 + EventBus(内存队列) + 预处理管线(清洗/转换/路由) | 接收 → 清洗 → 写入 ClickHouse 全链路通 |
| **数据存储** | ClickHouse明细表 + 聚合表 + InfluxDB 5个Measurement + Grafana Dashboard(JSON导入即用) | Dashboard 展示虚拟线程实时吞吐/延迟/活跃数/调度/错误率 |
| **部署** | Docker Compose 一键启动(Backend + ClickHouse + InfluxDB + Grafana) | 单机 `docker compose up` 即可跑通 |
| **安全** | Token 认证 + 应用白名单 | 未认证请求拒绝 |

### 8.2 v0.7 — 告警 + 分析引擎

| 模块 | 交付内容 |
|---|---|
| **告警引擎** | 5 种统计异常检测(开箱即用) + 钉钉/企业微信/邮件 Webhook + 收敛/静默/升级 |
| **分析引擎** | 阻塞根因分析 + 性能趋势分析 + 慢任务检测 |
| **后端增强** | EventBus → Kafka 实现(可选)、规则引擎热加载 |

### 8.3 v1.0 — 智能诊断

| 模块 | 交付内容 |
|---|---|
| **智能体** | MCP 工具(RAG检索/线程Dump/阻塞分析/错误查询) + LLM 对话诊断 + 向量库(Milvus/PGVector) |
| **安全增强** | mTLS + RBAC |
| **部署增强** | K8s Helm Chart |

### 8.2 非功能需求（全版本适用）

| 类别 | 要求 |
|---|---|
| **Agent 性能开销** | CPU < 3%, 内存 < 64MB, 启动延迟 < 500ms |
| **数据延迟** | Agent 事件 → ClickHouse 可查 P99 < 10s |
| **可用性** | 后端集群无单点故障，Agent 离线不影响业务应用 |
| **可扩展性** | EventBus/告警渠道/规则引擎均接口化，插件式扩展 |
| **兼容性** | JDK 21+, Linux/macOS/Windows(Agent) |

### 8.3 项目结构（建议）

```
dl-watching/
├── agent/                    # Java Agent 模块
│   ├── core/                 # ASM Hook 核心
│   ├── cache/                # 环形缓冲区
│   ├── reporter/             # gRPC 上报客户端
│   └── config/               # 配置管理
├── backend/                  # 后端服务模块
│   ├── gateway/              # gRPC/HTTP 网关
│   ├── pipeline/             # 数据预处理管线
│   ├── storage/              # ClickHouse/InfluxDB 写入器
│   ├── alert/                # 告警引擎
│   │   ├── detector/         # 统计异常检测
│   │   ├── rule/             # 规则引擎
│   │   └── channel/          # 告警渠道适配器
│   ├── analytics/            # 数据分析引擎
│   └── agent/                # 智能体模块(三期)
├── proto/                    # Protobuf 定义
├── dashboard/                # Grafana Dashboard JSON
├── deploy/                   # Docker Compose / K8s 部署配置
└── docs/                     # 文档
```

---

> 文档版本: v1.0 | 最后更新: 2026-05-30
