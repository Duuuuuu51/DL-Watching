# DL-Watching 开发进度日志

> 记录每个模块的完成情况、提交记录和技术决策，供后续开发参考。

---

## M1: 项目脚手架 (2026-05-30)

| 项目 | 内容 |
|---|---|
| 状态 | ✅ 完成 |
| 提交数 | 6 (5 任务 + 1 审查修复) |

### 提交记录

| Hash | 说明 |
|---|---|
| `82f756f` | M1.1: Parent POM with dependencyManagement / 父 POM 依赖管理 |
| `c693a98` | M1.2: Proto module with vt_monitor.proto / Proto 模块与消息定义 |
| `621073a` | M1.3: ProtobufSerializationTest / 序列化测试 |
| `7a84f34` | M1.4: Agent POM with shade plugin / Agent 模块与 shade 打包 |
| `a73518d` | M1.5: Backend POM with Spring Boot / Backend 模块与 Spring Boot |
| `d5d18ee` | M1: Code review fixes / 审查修复 (Javadoc, sub-packages, gitignore) |

### 产出文件

- `pom.xml` — 父 POM，含完整 dependencyManagement 和 pluginManagement
- `.gitignore` — Maven/IDE/OS 忽略规则
- `proto/pom.xml` — Proto 模块，protobuf-maven-plugin
- `proto/src/main/proto/vt_monitor.proto` — 10 个消息类型 + 3 个 RPC 服务
- `agent/pom.xml` — Agent 模块，maven-shade-plugin 带包重定位
- `agent/src/main/java/.../VtMonitorAgent.java` — premain 入口桩
- `backend/pom.xml` — Backend 模块，Spring Boot + gRPC starter
- `backend/src/main/java/.../BackendApplication.java` — Spring Boot 启动类
- `backend/src/main/resources/application.yml` — 配置 (8080/9090)

### 测试

| 模块 | 测试数 | 状态 |
|---|---|---|
| proto | 5 | ✅ 全部通过 |
| backend | 1 | ✅ 全部通过 |

### 审查发现

- spec 要求的 `logback-classic 1.5.6` 与 Spring Boot 3.2.5 不兼容，实际不需要 (SB 自带)
- 补齐了 agent 子包目录 (compat/, model/, hook/)
- 补齐了 Javadoc 和 gitignore 规则

---

## M2: Agent ASM Hook Framework (2026-06-01)

| 项目 | 内容 |
|---|---|
| 状态 | ✅ 完成 |
| 提交数 | 8 (6 任务 + 1 审查修复 + 1 author/docs) |

### 提交记录

| Hash | 说明 |
|---|---|
| `a77b32e` | M2.1: JdkCompat / JDK 版本检测 |
| `1a286a4` | M2.2: EventCollector interface / 事件收集器接口 |
| `2cf0df8` | M2.3: AbstractVtHookVisitor / ASM 访问器基类 |
| `03d06d0` | M2.3b: Stub VtLifecycle/VtScheduling visitors / 钩子桩实现 |
| `4b08527` | M2.4: VtClassFileTransformer / 字节码转换器 |
| `5a87089` | M2.5: VtMonitorAgent premain / Agent 入口完善 |
| `5c9683f` | M2: Code review fixes / 审查修复 (Javadoc, 单例) |
| `223df37` | docs: @author + README + rules / 作者标记和文档 |

### 产出文件

- `compat/JdkCompat.java` — JDK 版本检测，VirtualThread 内部类名常量
- `model/EventCollector.java` — 事件收集接口 + NoopEventCollector 单例
- `hook/AbstractVtHookVisitor.java` — ThreadLocal 收集器 + buildEvent 工厂
- `hook/VtLifecycleHookVisitor.java` — 生命周期钩子桩 (M3 实现)
- `hook/VtSchedulingHookVisitor.java` — 调度钩子桩 (M4 实现)
- `VtClassFileTransformer.java` — ClassFileTransformer，ASM 字节码转换
- `VtMonitorAgent.java` — 完整 premain，JDK 检查 + Transformer 注册

### 技术决策

- **ClassReader(byte[]) 替代 ClassReader(InputStream)**：更简洁，减少不必要的包装
- **NoopEventCollector 单例模式**：加 `resetNoop()` 解决测试间状态污染
- **桩访问器不重写任何方法**：在 M3/M4 实现前，ASM 链路可编译但不修改字节码
- **javax.annotation-api 1.3.2**：gRPC 生成的类需要 @javax.annotation.Generated

### 冒烟测试结果

- Agent JAR 成功加载，Transformer 注册成功
- VirtualThread 字节码拦截无报错，5 个虚拟线程正常创建调度
- Backend Spring Boot 启动成功，Tomcat 8080 + gRPC 9090

---

## 进度总览

| 模块 | 状态 | 完成日期 |
|---|---|---|
| M1: 项目脚手架 | ✅ | 2026-05-30 |
| M2: Agent 框架 | ✅ | 2026-06-01 |
| M3: 生命周期钩子 | ⏳ | — |
| M4: 调度钩子 | ⏳ | — |
| M5: 缓存与上报 | ⏳ | — |
| M6: 后端网关 | ⏳ | — |
| M7: 验证管道 | ⏳ | — |
| M8: 丰富与路由 | ⏳ | — |
| M9: 存储写入 | ⏳ | — |
| M10: 告警与分析 | ⏳ | — |
