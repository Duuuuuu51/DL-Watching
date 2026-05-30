# DL-Watching 虚拟线程监控系统 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan module-by-module.

**Goal:** 构建 Java 虚拟线程监控开源框架 v0.5 MVP。

**Architecture:** Layer 1 Agent(ASM+Protobuf+gRPC) → Layer 2 Backend(Spring Boot+管线) → Layer 3 Storage(ClickHouse+InfluxDB)+Grafana

**Tech Stack:** Java 21 / Maven 3.9+ / ASM 9.7 / Protobuf 3.25 / gRPC 1.68 / Spring Boot 3.2 / ClickHouse JDBC 0.6 / InfluxDB Client 6.12 / Testcontainers 1.19 / JUnit 5

---

## 执行纪律

- 模块**严格串行**（M1 → M2 → ... → M10），前一模块全部测试通过后才进入下一模块
- 同一模块内的 Tasks，sub-agent **并行处理不同 Task**
- 每个 Task 遵循 TDD：写测试 → 验证失败 → 实现 → 验证通过 → 提交

## 模块总览

| # | 模块 | 详情文件 | 交付物 |
|---|---|---|---|
| M1 | Project Scaffolding & Proto | [plan-01-project-scaffold.md](plan-01-project-scaffold.md) | Maven多模块+Proto生成 |
| M2 | Agent ASM Hook Framework | [plan-02-agent-framework.md](plan-02-agent-framework.md) | premain+Transformer+Visitor |
| M3 | Agent Lifecycle Hooks | [plan-03-agent-lifecycle-hooks.md](plan-03-agent-lifecycle-hooks.md) | create/start/terminate |
| M4 | Agent Scheduling Hooks | [plan-04-agent-scheduling-hooks.md](plan-04-agent-scheduling-hooks.md) | park/unpark/mount/unmount |
| M5 | Agent Cache & gRPC Reporter | [plan-05-agent-cache-reporter.md](plan-05-agent-cache-reporter.md) | 环形缓冲区+批量上报+重试 |
| M6 | Backend gRPC Gateway | [plan-06-backend-gateway.md](plan-06-backend-gateway.md) | Server+Auth+RateLimit |
| M7 | Backend Validation Pipeline | [plan-07-backend-pipeline.md](plan-07-backend-pipeline.md) | 校验+清洗+转换 |
| M8 | Backend Enrichment & EventBus | [plan-08-backend-enrich-eventbus.md](plan-08-backend-enrich-eventbus.md) | 丰富+聚合+路由 |
| M9 | Storage Writers | [plan-09-storage-writers.md](plan-09-storage-writers.md) | ClickHouse+InfluxDB |
| M10 | Alert + Analysis + Deploy | [plan-10-alert-analysis-deploy.md](plan-10-alert-analysis-deploy.md) | 异常检测+根因+部署 |

## 依赖链

```
M1(Proto) → M2(Framework) → M3(Lifecycle) → M4(Scheduling) → M5(Reporter)
                                                                  ↓
                                                             M6(Gateway) → M7(Pipeline) → M8(EventBus) → M9(Storage) → M10(Finals)
```

## 每模块完成检查

- [ ] `mvn clean test -pl <module>` 全部通过
- [ ] 无编译警告 (`-Xlint:all`)
- [ ] git commit 已提交

---

> Plan version: v2.0 | Last updated: 2026-05-30
