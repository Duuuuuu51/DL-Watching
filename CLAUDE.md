# DL-Watching 项目规则

## Git 提交规范

- 提交信息必须同时包含**中文和英文**两种描述
- 格式：`模块编号: 英文简述 / 中文简述`

示例：
```
M2.1: Add JdkCompat for JDK version detection and internal class names / 添加 JdkCompat JDK版本检测与内部类名常量
M2.2: Add EventCollector interface with no-op implementation / 添加 EventCollector 接口及空实现
```

## 技术栈

- Java 21, Maven 3.9+
- ASM 9.7, Protobuf 3.25.5, gRPC 1.68.0
- Spring Boot 3.2.5
- 测试: JUnit 5.10.2, AssertJ 3.25.3, Testcontainers 1.19.8

## 项目结构

```
dl-watching/
├── proto/       # Protobuf 定义 + 代码生成
├── agent/       # Java Agent (ASM 字节码增强)
└── backend/     # Spring Boot 后端 (gRPC 网关 + 数据处理)
```

## 代码规范

- 所有 Java 类必须添加 `@author` 注解：`@author Duuuuuu <1617714380@qq.com>`
- 放在类 Javadoc 之后、类签名之前

## 开发流程

- 子代理驱动开发 (Subagent-Driven Development)
- 每个模块完成后暂停，等待用户指令
- TDD: 先写测试 → 验证失败 → 实现 → 验证通过 → 提交
- 每个任务独立提交
