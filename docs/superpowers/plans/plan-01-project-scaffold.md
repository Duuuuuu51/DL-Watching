# M1: Project Scaffolding & Proto

> **Module:** M1 | **Dependencies:** None | **Status:** Draft

## Overview

Set up the Maven multi-module project structure with parent POM, proto module (Protobuf definitions + code generation), and stub modules for agent and backend.

## Task 1.1: Parent POM with dependencyManagement

- [ ] Create the root `pom.xml` at `D:\java-project\DL-Watching\pom.xml`

**Step 1.1.1:** Write the root pom.xml:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.github.dlwatching</groupId>
    <artifactId>dl-watching</artifactId>
    <version>0.5.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>DL-Watching Virtual Thread Monitor</name>
    <description>Java Virtual Thread monitoring framework with ASM bytecode enhancement, gRPC communication, and multi-storage backend.</description>

    <modules>
        <module>proto</module>
        <module>agent</module>
        <module>backend</module>
    </modules>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <asm.version>9.7</asm.version>
        <protobuf.version>3.25.5</protobuf.version>
        <grpc.version>1.68.0</grpc.version>
        <spring-boot.version>3.2.5</spring-boot.version>
        <clickhouse-jdbc.version>0.6.2</clickhouse-jdbc.version>
        <influxdb-client.version>6.12.0</influxdb-client.version>
        <testcontainers.version>1.19.8</testcontainers.version>
        <junit.version>5.10.2</junit.version>
        <assertj.version>3.25.3</assertj.version>
        <guava.version>33.2.0-jre</guava.version>
        <jakarta-annotation.version>2.1.1</jakarta-annotation.version>
        <maven-shade-plugin.version>3.5.2</maven-shade-plugin.version>
        <protobuf-maven-plugin.version>0.6.1</protobuf-maven-plugin.version>
        <os-maven-plugin.version>1.7.1</os-maven-plugin.version>
        <maven-compiler-plugin.version>3.12.1</maven-compiler-plugin.version>
        <maven-surefire-plugin.version>3.2.5</maven-surefire-plugin.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <!-- ASM -->
            <dependency>
                <groupId>org.ow2.asm</groupId>
                <artifactId>asm</artifactId>
                <version>${asm.version}</version>
            </dependency>
            <dependency>
                <groupId>org.ow2.asm</groupId>
                <artifactId>asm-commons</artifactId>
                <version>${asm.version}</version>
            </dependency>
            <dependency>
                <groupId>org.ow2.asm</groupId>
                <artifactId>asm-util</artifactId>
                <version>${asm.version}</version>
            </dependency>

            <!-- Protobuf -->
            <dependency>
                <groupId>com.google.protobuf</groupId>
                <artifactId>protobuf-java</artifactId>
                <version>${protobuf.version}</version>
            </dependency>
            <dependency>
                <groupId>com.google.protobuf</groupId>
                <artifactId>protobuf-java-util</artifactId>
                <version>${protobuf.version}</version>
            </dependency>

            <!-- gRPC -->
            <dependency>
                <groupId>io.grpc</groupId>
                <artifactId>grpc-protobuf</artifactId>
                <version>${grpc.version}</version>
            </dependency>
            <dependency>
                <groupId>io.grpc</groupId>
                <artifactId>grpc-stub</artifactId>
                <version>${grpc.version}</version>
            </dependency>
            <dependency>
                <groupId>io.grpc</groupId>
                <artifactId>grpc-netty-shaded</artifactId>
                <version>${grpc.version}</version>
            </dependency>
            <dependency>
                <groupId>io.grpc</groupId>
                <artifactId>grpc-services</artifactId>
                <version>${grpc.version}</version>
            </dependency>

            <!-- Guava (needed by gRPC) -->
            <dependency>
                <groupId>com.google.guava</groupId>
                <artifactId>guava</artifactId>
                <version>${guava.version}</version>
            </dependency>

            <!-- Jakarta Annotation (for gRPC @Generated) -->
            <dependency>
                <groupId>jakarta.annotation</groupId>
                <artifactId>jakarta.annotation-api</artifactId>
                <version>${jakarta-annotation.version}</version>
            </dependency>

            <!-- Spring Boot -->
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>

            <!-- gRPC Server Spring Boot Starter -->
            <dependency>
                <groupId>net.devh</groupId>
                <artifactId>grpc-server-spring-boot-starter</artifactId>
                <version>3.0.0.RELEASE</version>
            </dependency>

            <!-- ClickHouse JDBC -->
            <dependency>
                <groupId>com.clickhouse</groupId>
                <artifactId>clickhouse-jdbc</artifactId>
                <version>${clickhouse-jdbc.version}</version>
            </dependency>

            <!-- InfluxDB Client -->
            <dependency>
                <groupId>com.influxdb</groupId>
                <artifactId>influxdb-client-java</artifactId>
                <version>${influxdb-client.version}</version>
            </dependency>

            <!-- Testcontainers -->
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>

            <!-- JUnit 5 -->
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>

            <!-- AssertJ -->
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>${assertj.version}</version>
                <scope>test</scope>
            </dependency>

            <!-- Logback (for tests) -->
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>1.5.6</version>
                <scope>test</scope>
            </dependency>

            <!-- Module-local proto dependency -->
            <dependency>
                <groupId>io.github.dlwatching</groupId>
                <artifactId>dl-watching-proto</artifactId>
                <version>${project.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>${maven-compiler-plugin.version}</version>
                    <configuration>
                        <source>${maven.compiler.source}</source>
                        <target>${maven.compiler.target}</target>
                        <parameters>true</parameters>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>${maven-surefire-plugin.version}</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-shade-plugin</artifactId>
                    <version>${maven-shade-plugin.version}</version>
                </plugin>
                <plugin>
                    <groupId>org.xolstice.maven.plugins</groupId>
                    <artifactId>protobuf-maven-plugin</artifactId>
                    <version>${protobuf-maven-plugin.version}</version>
                    <configuration>
                        <protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
                        <pluginId>grpc-java</pluginId>
                        <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}</pluginArtifact>
                    </configuration>
                    <executions>
                        <execution>
                            <goals>
                                <goal>compile</goal>
                                <goal>compile-custom</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
                <plugin>
                    <groupId>kr.motd.maven</groupId>
                    <artifactId>os-maven-plugin</artifactId>
                    <version>${os-maven-plugin.version}</version>
                    <extensions>true</extensions>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [ ] Run the following command to verify the POM parses correctly:

```bash
cd D:\java-project\DL-Watching && mvn validate
```

**Expected result:** BUILD SUCCESS. No modules exist yet but the parent POM is valid XML.

- [ ] Create `D:\java-project\DL-Watching\.gitignore` with the following content (append if file exists):

```gitignore
# Maven
target/
*.class
*.jar
*.war
!agent/target/*.jar

# IDE
.idea/
*.iml
*.ipr
*.iws

# OS
.DS_Store
Thumbs.db

# Logs
*.log

# Temp
*.tmp
*.swp
*~
```

- [ ] Stage and commit the parent POM:

```bash
cd D:\java-project\DL-Watching && git add pom.xml .gitignore && git commit -m "$(cat <<'EOF'
M1.1: Add parent POM with dependencyManagement for all modules

Define versions for ASM 9.7, Protobuf 3.25.5, gRPC 1.68.0, Spring Boot 3.2.5,
ClickHouse JDBC 0.6.2, InfluxDB 6.12.0, Testcontainers 1.19.8, plus build plugins.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

## Task 1.2: Proto module POM + vt_monitor.proto

- [ ] Create directory: `D:\java-project\DL-Watching\proto\src\main\proto`

```bash
mkdir -p "D:\java-project\DL-Watching\proto\src\main\proto"
```

- [ ] Create `D:\java-project\DL-Watching\proto\pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>io.github.dlwatching</groupId>
        <artifactId>dl-watching</artifactId>
        <version>0.5.0-SNAPSHOT</version>
    </parent>

    <artifactId>dl-watching-proto</artifactId>
    <packaging>jar</packaging>
    <name>DL-Watching Proto Definitions</name>
    <description>Protobuf message definitions and generated Java code for the Virtual Thread Monitor.</description>

    <dependencies>
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-protobuf</artifactId>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-stub</artifactId>
        </dependency>
        <dependency>
            <groupId>jakarta.annotation</groupId>
            <artifactId>jakarta.annotation-api</artifactId>
        </dependency>
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
        </dependency>
    </dependencies>

    <build>
        <extensions>
            <extension>
                <groupId>kr.motd.maven</groupId>
                <artifactId>os-maven-plugin</artifactId>
            </extension>
        </extensions>
        <plugins>
            <plugin>
                <groupId>org.xolstice.maven.plugins</groupId>
                <artifactId>protobuf-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] Create `D:\java-project\DL-Watching\proto\src\main\proto\vt_monitor.proto`:

```protobuf
syntax = "proto3";

package dlwatching.proto;

option java_package = "io.github.dlwatching.proto";
option java_multiple_files = true;

// ============================================================
// Enums
// ============================================================

enum EventType {
  EVENT_TYPE_UNSPECIFIED = 0;
  CREATED = 1;
  STARTED = 2;
  MOUNTED = 3;
  UNMOUNTED = 4;
  PARKED = 5;
  UNPARKED = 6;
  TERMINATED = 7;
  HEARTBEAT = 8;
}

// ============================================================
// Agent Register
// ============================================================

message RegisterRequest {
  string app_id = 1;
  string instance_id = 2;      // hostname_pid
  string jdk_version = 3;
  string agent_version = 4;
  string auth_token = 5;
}

message RegisterResponse {
  string session_token = 1;
  AgentConfig config = 2;
}

// ============================================================
// Agent Configuration
// ============================================================

message AgentConfig {
  int32 batch_size = 1;
  int32 flush_interval_ms = 2;
  double sample_rate = 3;
  bool hook_enabled = 4;
  string log_level = 5;
}

// ============================================================
// Events
// ============================================================

message StackFrame {
  string class_name = 1;
  string method_name = 2;
  string file_name = 3;
  int32 line_number = 4;
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
}

message EventBatch {
  string app_id = 1;
  string instance_id = 2;
  int64 batch_seq = 3;
  int64 timestamp_ms = 4;
  repeated ThreadEvent events = 5;
}

// ============================================================
// Control Commands (from Backend to Agent)
// ============================================================

message ThreadQueryParams {
  int64 thread_id = 1;
  string app_id = 2;
}

message ThreadQueryResult {
  int64 thread_id = 1;
  string thread_name = 2;
  string state = 3;
  int64 created_at_ms = 4;
  repeated StackFrame stack_frames = 5;
}

message ControlCommand {
  CommandType type = 1;
  string command_id = 2;

  oneof payload {
    AgentConfig new_config = 3;
    ThreadQueryParams query_params = 4;
  }

  enum CommandType {
    ACK = 0;
    SLOW_DOWN = 1;
    SPEED_UP = 2;
    QUERY_THREAD = 3;
    DUMP_THREADS = 4;
    UPDATE_CONFIG = 5;
  }
}

// ============================================================
// Heartbeat
// ============================================================

message HeartbeatRequest {
  string app_id = 1;
  string instance_id = 2;
  string session_token = 3;
  int64 timestamp_ms = 4;
  int64 collected_count = 5;
  int64 dropped_count = 6;
  int64 sent_count = 7;
}

message HeartbeatResponse {
  int64 server_timestamp_ms = 1;
  AgentConfig updated_config = 2;
}

// ============================================================
// Service Definition
// ============================================================

service VirtualThreadMonitor {
  // Agent registers with the backend and receives session token + config.
  rpc Register(RegisterRequest) returns (RegisterResponse);

  // Bidirectional stream: Agent sends EventBatch, Backend sends ControlCommand.
  rpc Report(stream EventBatch) returns (stream ControlCommand);

  // Periodic heartbeat to maintain session liveness.
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
```

- [ ] Run compilation to generate Java classes from proto:

```bash
cd D:\java-project\DL-Watching && mvn clean compile -pl proto
```

**Expected result:** BUILD SUCCESS. Java files generated under `proto/target/generated-sources/protobuf/` with classes like `ThreadEvent`, `EventBatch`, `ControlCommand`, `VirtualThreadMonitorGrpc`.

- [ ] Verify generated classes exist:

```bash
ls -la "D:\java-project\DL-Watching\proto\target\generated-sources\protobuf\java\io\github\dlwatching\proto\"
```

**Expected:** Files like `ThreadEvent.java`, `EventBatch.java`, `ControlCommand.java`, `VirtualThreadMonitorGrpc.java`, etc.

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add proto/ && git commit -m "$(cat <<'EOF'
M1.2: Add proto module with vt_monitor.proto and code generation

Define all message types: RegisterRequest/Response, AgentConfig, ThreadEvent,
EventBatch, ControlCommand, StackFrame, HeartbeatRequest/Response,
ThreadQueryParams/Result, EventType enum, and VirtualThreadMonitor gRPC service.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

## Task 1.3: ProtobufSerializationTest

- [ ] Create directory: `D:\java-project\DL-Watching\proto\src\test\java\io\github\dlwatching\proto`

```bash
mkdir -p "D:\java-project\DL-Watching\proto\src\test\java\io\github\dlwatching\proto"
```

- [ ] Create `D:\java-project\DL-Watching\proto\src\test\java\io\github\dlwatching\proto\ProtobufSerializationTest.java`:

```java
package io.github.dlwatching.proto;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtobufSerializationTest {

    @Test
    void shouldSerializeAndDeserializeThreadEvent() throws InvalidProtocolBufferException {
        StackFrame caller = StackFrame.newBuilder()
                .setClassName("com.example.MyService")
                .setMethodName("processOrder")
                .setFileName("MyService.java")
                .setLineNumber(42)
                .build();

        ThreadEvent original = ThreadEvent.newBuilder()
                .setType(EventType.CREATED)
                .setThreadId(1001L)
                .setThreadName("vt-1001")
                .setTimestampMs(System.currentTimeMillis())
                .setCarrierThread("ForkJoinPool-1-worker-1")
                .setDurationUs(0L)
                .setReason("")
                .setCaller(caller)
                .build();

        byte[] bytes = original.toByteArray();
        ThreadEvent deserialized = ThreadEvent.parseFrom(bytes);

        assertThat(deserialized.getType()).isEqualTo(EventType.CREATED);
        assertThat(deserialized.getThreadId()).isEqualTo(1001L);
        assertThat(deserialized.getThreadName()).isEqualTo("vt-1001");
        assertThat(deserialized.getCarrierThread()).isEqualTo("ForkJoinPool-1-worker-1");
        assertThat(deserialized.getCaller()).isNotNull();
        assertThat(deserialized.getCaller().getClassName()).isEqualTo("com.example.MyService");
        assertThat(deserialized.getCaller().getMethodName()).isEqualTo("processOrder");
        assertThat(deserialized.getCaller().getLineNumber()).isEqualTo(42);
    }

    @Test
    void shouldSerializeAndDeserializeEventBatch() throws InvalidProtocolBufferException {
        ThreadEvent event1 = ThreadEvent.newBuilder()
                .setType(EventType.CREATED)
                .setThreadId(1L)
                .setThreadName("vt-1")
                .setTimestampMs(1000L)
                .build();

        ThreadEvent event2 = ThreadEvent.newBuilder()
                .setType(EventType.STARTED)
                .setThreadId(1L)
                .setThreadName("vt-1")
                .setTimestampMs(1005L)
                .build();

        EventBatch original = EventBatch.newBuilder()
                .setAppId("test-app")
                .setInstanceId("host-1_12345")
                .setBatchSeq(1L)
                .setTimestampMs(System.currentTimeMillis())
                .addAllEvents(List.of(event1, event2))
                .build();

        byte[] bytes = original.toByteArray();
        EventBatch deserialized = EventBatch.parseFrom(bytes);

        assertThat(deserialized.getAppId()).isEqualTo("test-app");
        assertThat(deserialized.getInstanceId()).isEqualTo("host-1_12345");
        assertThat(deserialized.getBatchSeq()).isEqualTo(1L);
        assertThat(deserialized.getEventsCount()).isEqualTo(2);
        assertThat(deserialized.getEvents(0).getType()).isEqualTo(EventType.CREATED);
        assertThat(deserialized.getEvents(1).getType()).isEqualTo(EventType.STARTED);
    }

    @Test
    void shouldSerializeAndDeserializeControlCommand() throws InvalidProtocolBufferException {
        AgentConfig config = AgentConfig.newBuilder()
                .setBatchSize(1000)
                .setFlushIntervalMs(5000)
                .setSampleRate(0.1)
                .setHookEnabled(true)
                .setLogLevel("DEBUG")
                .build();

        ControlCommand original = ControlCommand.newBuilder()
                .setType(ControlCommand.CommandType.UPDATE_CONFIG)
                .setCommandId("cmd-001")
                .setNewConfig(config)
                .build();

        byte[] bytes = original.toByteArray();
        ControlCommand deserialized = ControlCommand.parseFrom(bytes);

        assertThat(deserialized.getType()).isEqualTo(ControlCommand.CommandType.UPDATE_CONFIG);
        assertThat(deserialized.getCommandId()).isEqualTo("cmd-001");
        assertThat(deserialized.hasNewConfig()).isTrue();
        assertThat(deserialized.getNewConfig().getBatchSize()).isEqualTo(1000);
        assertThat(deserialized.getNewConfig().getFlushIntervalMs()).isEqualTo(5000);
        assertThat(deserialized.getNewConfig().getSampleRate()).isEqualTo(0.1);
        assertThat(deserialized.getNewConfig().getHookEnabled()).isTrue();
        assertThat(deserialized.getNewConfig().getLogLevel()).isEqualTo("DEBUG");
    }

    @Test
    void shouldHandleEmptyEventBatch() throws InvalidProtocolBufferException {
        EventBatch original = EventBatch.newBuilder()
                .setAppId("empty-test")
                .setInstanceId("host-1")
                .setBatchSeq(0L)
                .setTimestampMs(1234L)
                .build();

        byte[] bytes = original.toByteArray();
        EventBatch deserialized = EventBatch.parseFrom(bytes);

        assertThat(deserialized.getAppId()).isEqualTo("empty-test");
        assertThat(deserialized.getEventsCount()).isEqualTo(0);
        assertThat(bytes.length).isLessThan(50); // very small serialized size
    }

    @Test
    void shouldBuildThreadEventWithOnlyRequiredFields() throws InvalidProtocolBufferException {
        ThreadEvent original = ThreadEvent.newBuilder()
                .setType(EventType.HEARTBEAT)
                .setThreadId(0L)
                .setThreadName("")
                .setTimestampMs(9999L)
                .build();

        byte[] bytes = original.toByteArray();
        ThreadEvent deserialized = ThreadEvent.parseFrom(bytes);

        assertThat(deserialized.getType()).isEqualTo(EventType.HEARTBEAT);
        assertThat(deserialized.getThreadId()).isZero();
        assertThat(deserialized.getTimestampMs()).isEqualTo(9999L);
        // Optional fields should return defaults
        assertThat(deserialized.getCarrierThread()).isEmpty();
        assertThat(deserialized.getDurationUs()).isZero();
        assertThat(deserialized.getReason()).isEmpty();
        assertThat(deserialized.hasCaller()).isFalse();
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl proto -Dtest=ProtobufSerializationTest
```

**Expected result:** Tests PASS (5/5). All serialization/deserialization round-trips pass.

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add proto/ && git commit -m "$(cat <<'EOF'
M1.3: Add ProtobufSerializationTest for ThreadEvent, EventBatch, ControlCommand

Verify serialization round-trip, empty batch handling, and partial-field events.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

## Task 1.4: Agent module POM with shade plugin

- [ ] Create directories:

```bash
mkdir -p "D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent"
mkdir -p "D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\compat"
mkdir -p "D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\model"
mkdir -p "D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\hook"
mkdir -p "D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent"
```

- [ ] Create `D:\java-project\DL-Watching\agent\pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>io.github.dlwatching</groupId>
        <artifactId>dl-watching</artifactId>
        <version>0.5.0-SNAPSHOT</version>
    </parent>

    <artifactId>dl-watching-agent</artifactId>
    <packaging>jar</packaging>
    <name>DL-Watching Java Agent</name>
    <description>Java Agent for Virtual Thread monitoring using ASM bytecode enhancement.</description>

    <dependencies>
        <!-- Proto definitions -->
        <dependency>
            <groupId>io.github.dlwatching</groupId>
            <artifactId>dl-watching-proto</artifactId>
        </dependency>

        <!-- ASM -->
        <dependency>
            <groupId>org.ow2.asm</groupId>
            <artifactId>asm</artifactId>
        </dependency>
        <dependency>
            <groupId>org.ow2.asm</groupId>
            <artifactId>asm-commons</artifactId>
        </dependency>
        <dependency>
            <groupId>org.ow2.asm</groupId>
            <artifactId>asm-util</artifactId>
        </dependency>

        <!-- gRPC (will be shaded) -->
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-protobuf</artifactId>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-stub</artifactId>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-netty-shaded</artifactId>
        </dependency>

        <!-- Protobuf (will be shaded) -->
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
        </dependency>

        <!-- Guava (will be shaded) -->
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
        </dependency>

        <!-- Jakarta Annotation -->
        <dependency>
            <groupId>jakarta.annotation</groupId>
            <artifactId>jakarta.annotation-api</artifactId>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <shadedArtifactAttached>false</shadedArtifactAttached>
                            <createDependencyReducedPom>true</createDependencyReducedPom>
                            <relocations>
                                <relocation>
                                    <pattern>com.google.protobuf</pattern>
                                    <shadedPattern>dlwatching.shaded.com.google.protobuf</shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.google.common</pattern>
                                    <shadedPattern>dlwatching.shaded.com.google.common</shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>io.grpc</pattern>
                                    <shadedPattern>dlwatching.shaded.io.grpc</shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>io.netty</pattern>
                                    <shadedPattern>dlwatching.shaded.io.netty</shadedPattern>
                                </relocation>
                            </relocations>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/MANIFEST.MF</exclude>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <manifestEntries>
                                        <Premain-Class>io.github.dlwatching.agent.VtMonitorAgent</Premain-Class>
                                        <Can-Retransform-Classes>true</Can-Retransform-Classes>
                                        <Can-Set-Native-Method-Prefix>true</Can-Set-Native-Method-Prefix>
                                        <Agent-Class>io.github.dlwatching.agent.VtMonitorAgent</Agent-Class>
                                    </manifestEntries>
                                </transformer>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] Create a minimal `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\VtMonitorAgent.java` (stub so the module compiles):

```java
package io.github.dlwatching.agent;

import java.lang.instrument.Instrumentation;

/**
 * Entry point for the DL-Watching Java Agent.
 *
 * <p>Registered as {@code Premain-Class} in the shaded JAR manifest.
 * This stub will be fully implemented in M2.
 */
public final class VtMonitorAgent {

    public static final String VERSION = "0.5.0-SNAPSHOT";

    private VtMonitorAgent() {
        // utility class
    }

    /**
     * JVM agent premain entry point (Java 21+).
     *
     * @param agentArgs  agent command-line arguments
     * @param inst       instrumentation instance
     */
    public static void premain(String agentArgs, Instrumentation inst) {
        System.out.println("[DL-Watching Agent] v" + VERSION + " initializing...");
        // Full implementation will be added in M2.
    }
}
```

- [ ] Compile the agent module:

```bash
cd D:\java-project\DL-Watching && mvn clean compile -pl agent
```

**Expected result:** BUILD SUCCESS.

- [ ] Package the agent JAR (with shading):

```bash
cd D:\java-project\DL-Watching && mvn clean package -pl agent
```

**Expected result:** BUILD SUCCESS. File `agent/target/dl-watching-agent-0.5.0-SNAPSHOT.jar` created.

- [ ] Verify the shaded JAR manifest:

```bash
jar tf "D:\java-project\DL-Watching\agent\target\dl-watching-agent-0.5.0-SNAPSHOT.jar" META-INF/MANIFEST.MF
```

**Expected:** Manifest contains `Premain-Class: io.github.dlwatching.agent.VtMonitorAgent`.

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add agent/ && git commit -m "$(cat <<'EOF'
M1.4: Add agent module POM with shade plugin and Premain-Class manifest

Configure relocation of protobuf, guava, grpc, netty into dlwatching.shaded
package to avoid classpath conflicts. Set Premain-Class to VtMonitorAgent.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

## Task 1.5: Backend module POM with Spring Boot + gRPC server starter

- [ ] Create directories:

```bash
mkdir -p "D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend"
mkdir -p "D:\java-project\DL-Watching\backend\src\main\resources"
mkdir -p "D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend"
```

- [ ] Create `D:\java-project\DL-Watching\backend\pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>io.github.dlwatching</groupId>
        <artifactId>dl-watching</artifactId>
        <version>0.5.0-SNAPSHOT</version>
    </parent>

    <artifactId>dl-watching-backend</artifactId>
    <packaging>jar</packaging>
    <name>DL-Watching Backend</name>
    <description>Spring Boot backend for Virtual Thread monitoring with gRPC gateway, data pipeline, and storage writers.</description>

    <dependencies>
        <!-- Proto definitions -->
        <dependency>
            <groupId>io.github.dlwatching</groupId>
            <artifactId>dl-watching-proto</artifactId>
        </dependency>

        <!-- Spring Boot -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>

        <!-- gRPC Server Spring Boot Starter -->
        <dependency>
            <groupId>net.devh</groupId>
            <artifactId>grpc-server-spring-boot-starter</artifactId>
        </dependency>

        <!-- Protobuf -->
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
        </dependency>

        <!-- gRPC -->
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-protobuf</artifactId>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-stub</artifactId>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-services</artifactId>
        </dependency>

        <!-- Jakarta Annotation -->
        <dependency>
            <groupId>jakarta.annotation</groupId>
            <artifactId>jakarta.annotation-api</artifactId>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>repackage</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\resources\application.yml`:

```yaml
server:
  port: 8080

spring:
  application:
    name: dl-watching-backend

grpc:
  server:
    port: 9090
    enable-keep-alive: true
    keep-alive-time: 30s
    keep-alive-timeout: 10s
    max-inbound-message-size: 4194304  # 4MB

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
  endpoint:
    health:
      show-details: always
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\BackendApplication.java`:

```java
package io.github.dlwatching.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the DL-Watching Backend service.
 *
 * <p>Spring Boot application with embedded gRPC server for receiving
 * Virtual Thread monitoring data from Java Agents.
 */
@SpringBootApplication
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\BackendApplicationTest.java`:

```java
package io.github.dlwatching.backend;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest
class BackendApplicationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void contextLoads() {
        assertThat(applicationContext).isNotNull();
    }
}
```

- [ ] Compile and test the backend module:

```bash
cd D:\java-project\DL-Watching && mvn clean test -pl backend
```

**Expected result:** BUILD SUCCESS. The Spring Boot context loads successfully. The health endpoint and gRPC server configuration are verified.

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add backend/ && git commit -m "$(cat <<'EOF'
M1.5: Add backend module POM with Spring Boot and gRPC server starter

Configure Spring Boot 3.2.5 with web, actuator, and grpc-server-spring-boot-starter.
Add application.yml with gRPC port 9090 and BackendApplication entry point.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

## M1 Completion Check

- [ ] Run full build:
```bash
cd D:\java-project\DL-Watching && mvn clean verify
```
**Expected:** BUILD SUCCESS (all sub-modules compile, all tests pass).

- [ ] Verify git log:
```bash
cd D:\java-project\DL-Watching && git log --oneline -5
```
**Expected:** 5 commits for M1 tasks 1.1 through 1.5.
