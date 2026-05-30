package io.github.dlwatching.proto;

import static org.assertj.core.api.Assertions.assertThat;

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
                .setCaller(caller)
                .build();
        byte[] bytes = original.toByteArray();
        ThreadEvent deserialized = ThreadEvent.parseFrom(bytes);
        assertThat(deserialized.getType()).isEqualTo(EventType.CREATED);
        assertThat(deserialized.getThreadId()).isEqualTo(1001L);
        assertThat(deserialized.getThreadName()).isEqualTo("vt-1001");
        assertThat(deserialized.getCarrierThread()).isEqualTo("ForkJoinPool-1-worker-1");
        assertThat(deserialized.getCaller().getClassName()).isEqualTo("com.example.MyService");
        assertThat(deserialized.getCaller().getMethodName()).isEqualTo("processOrder");
        assertThat(deserialized.getCaller().getLineNumber()).isEqualTo(42);
    }

    @Test
    void shouldSerializeAndDeserializeEventBatch() throws InvalidProtocolBufferException {
        ThreadEvent event1 = ThreadEvent.newBuilder().setType(EventType.CREATED).setThreadId(1L).setThreadName("vt-1").setTimestampMs(1000L).build();
        ThreadEvent event2 = ThreadEvent.newBuilder().setType(EventType.STARTED).setThreadId(1L).setThreadName("vt-1").setTimestampMs(1005L).build();
        EventBatch original = EventBatch.newBuilder()
                .setAppId("test-app").setInstanceId("host-1_12345").setBatchSeq(1L)
                .setTimestampMs(System.currentTimeMillis())
                .addAllEvents(List.of(event1, event2)).build();
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
        AgentConfig config = AgentConfig.newBuilder().setBatchSize(1000).setFlushIntervalMs(5000).setSampleRate(0.1).setHookEnabled(true).setLogLevel("DEBUG").build();
        ControlCommand original = ControlCommand.newBuilder()
                .setType(ControlCommand.CommandType.UPDATE_CONFIG).setCommandId("cmd-001").setNewConfig(config).build();
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
        EventBatch original = EventBatch.newBuilder().setAppId("empty-test").setInstanceId("host-1").setBatchSeq(0L).setTimestampMs(1234L).build();
        byte[] bytes = original.toByteArray();
        EventBatch deserialized = EventBatch.parseFrom(bytes);
        assertThat(deserialized.getAppId()).isEqualTo("empty-test");
        assertThat(deserialized.getEventsCount()).isEqualTo(0);
        assertThat(bytes.length).isLessThan(50);
    }

    @Test
    void shouldBuildThreadEventWithOnlyRequiredFields() throws InvalidProtocolBufferException {
        ThreadEvent original = ThreadEvent.newBuilder().setType(EventType.HEARTBEAT).setThreadId(0L).setThreadName("").setTimestampMs(9999L).build();
        byte[] bytes = original.toByteArray();
        ThreadEvent deserialized = ThreadEvent.parseFrom(bytes);
        assertThat(deserialized.getType()).isEqualTo(EventType.HEARTBEAT);
        assertThat(deserialized.getThreadId()).isZero();
        assertThat(deserialized.getTimestampMs()).isEqualTo(9999L);
        assertThat(deserialized.getCarrierThread()).isEmpty();
        assertThat(deserialized.getDurationUs()).isZero();
        assertThat(deserialized.getReason()).isEmpty();
        assertThat(deserialized.hasCaller()).isFalse();
    }
}
