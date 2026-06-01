# DL-Watching

Java Virtual Thread Monitoring System / Java 虚拟线程监控系统

基于 ASM 字节码增强的 Java 虚拟线程可观测性框架，支持实时事件采集、gRPC 上报、多存储后端与分析告警。

## Architecture / 架构

```
Layer 1: Java Agent (ASM + gRPC)     Layer 2: Backend (Spring Boot)     Layer 3: Storage
┌─────────────────────────┐         ┌──────────────────────────┐      ┌──────────────────┐
│ VirtualThread Hooks      │  gRPC   │ Gateway (Auth/Rate/CB)    │      │ ClickHouse        │
│ RingBuffer → Aggregator  │ ──────→ │ Validation → Clean → TF  │ ───→ │ (Event Detail)    │
│ GrpcReporter             │  Stream │ Enrichment → EventBus     │      │ InfluxDB (Metrics) │
└─────────────────────────┘         └──────────────────────────┘      └──────────────────┘
```

## Tech Stack / 技术栈

| Component 组件 | Technology 技术 |
|---|---|
| Language | Java 21 |
| Bytecode | ASM 9.7 |
| Serialization | Protobuf 3.25.5 |
| RPC | gRPC 1.68.0 (bidirectional streaming) |
| Backend | Spring Boot 3.2.5 |
| Storage | ClickHouse + InfluxDB |
| Test | JUnit 5, AssertJ, Testcontainers |

## Progress / 开发进度

| Module 模块 | Status 状态 | Description 说明 |
|---|---|---|
| M1: Project Scaffold / 项目脚手架 | ✅ Complete | Multi-module Maven, Proto definitions, POM configuration |
| M2: Agent Framework / Agent 框架 | ✅ Complete | JdkCompat, EventCollector, HookVisitor, ClassFileTransformer, Agent premain |
| M3: Lifecycle Hooks / 生命周期钩子 | ⏳ Pending | VirtualThread create/start/terminate hooks |
| M4: Scheduling Hooks / 调度钩子 | ⏳ Pending | park/unpark/mount/unmount hooks |
| M5: Cache & Reporter / 缓存与上报 | ⏳ Pending | RingBuffer, BatchAggregator, GrpcReporter |
| M6: Backend Gateway / 后端网关 | ⏳ Pending | gRPC auth, rate limiting, circuit breaker |
| M7: Validation Pipeline / 验证管道 | ⏳ Pending | 3-layer validation, cleaning, transformation |
| M8: Enrich & EventBus / 丰富与路由 | ⏳ Pending | Window aggregation, event routing |
| M9: Storage Writers / 存储写入 | ⏳ Pending | ClickHouse + InfluxDB writers |
| M10: Alert & Analysis / 告警与分析 | ⏳ Pending | Anomaly detection, root cause analysis, deploy |

## Quick Start / 快速开始

```bash
# Build
JAVA_HOME=/path/to/jdk-21 mvn clean verify

# Run with agent
java -javaagent:agent/target/dl-watching-agent-0.5.0-SNAPSHOT.jar \
     -jar your-application.jar

# Start backend
java -jar backend/target/dl-watching-backend-0.5.0-SNAPSHOT.jar
```

## License / 许可证

Open source under Apache License 2.0.
