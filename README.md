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
| RPC | gRPC 1.68.0 (bidirectional streaming / 双向流) |
| Backend | Spring Boot 3.2.5 |
| Storage | ClickHouse + InfluxDB |
| Test | JUnit 5, AssertJ, Testcontainers |

## Quick Start / 快速开始

```bash
# Build / 构建
JAVA_HOME=/path/to/jdk-21 mvn clean verify

# Run with agent / 挂载 Agent 启动
java -javaagent:agent/target/dl-watching-agent-0.5.0-SNAPSHOT.jar \
     -jar your-application.jar

# Start backend / 启动后端
java -jar backend/target/dl-watching-backend-0.5.0-SNAPSHOT.jar
```

## Project Structure / 项目结构

```
dl-watching/
├── proto/       # Protobuf definitions + code generation
├── agent/       # Java Agent (ASM bytecode enhancement)
└── backend/     # Spring Boot backend (gRPC gateway + data pipeline)
```

## License / 许可证

Apache License 2.0


