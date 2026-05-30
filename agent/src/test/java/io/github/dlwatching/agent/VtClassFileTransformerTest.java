package io.github.dlwatching.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.agent.model.EventCollector;
import org.junit.jupiter.api.Test;

class VtClassFileTransformerTest {

    @Test
    void shouldTargetVirtualThreadInternalName() {
        assertThat(VtClassFileTransformer.TARGET_INTERNAL_NAMES)
                .contains("java/lang/VirtualThread");
    }

    @Test
    void shouldOnlyContainVirtualThreadByDefault() {
        assertThat(VtClassFileTransformer.TARGET_INTERNAL_NAMES).hasSize(1);
    }

    @Test
    void shouldSkipNullClassName() throws Exception {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);
        byte[] result = transformer.transform(null, null, null, null, new byte[]{});
        assertThat(result).isNull();
    }

    @Test
    void shouldSkipNonTargetClass() throws Exception {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);
        byte[] result = transformer.transform(null, "com/example/MyClass", null, null, new byte[]{});
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnNullWhenDisabled() throws Exception {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);
        transformer.setEnabled(false);
        byte[] result = transformer.transform(null, "java/lang/VirtualThread", null, null, new byte[]{});
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnTransformedBytesForTargetClass() throws Exception {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);
        Class<?> vtClass = Class.forName("java.lang.VirtualThread");
        String vtInternalName = vtClass.getName().replace('.', '/');
        byte[] classBytes = vtClass.getResourceAsStream("/" + vtInternalName + ".class").readAllBytes();
        byte[] transformed = transformer.transform(null, "java/lang/VirtualThread", null, null, classBytes);
        assertThat(transformed).isNotNull();
        assertThat(transformed.length).isGreaterThan(0);
    }

    @Test
    void shouldDefaultToNoopCollectorWhenNullGiven() {
        VtClassFileTransformer transformer = new VtClassFileTransformer(null);
        assertThat(transformer.getCollector()).isNotNull();
    }

    @Test
    void shouldSupportDynamicCollectorSwap() {
        EventCollector collector1 = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector1);
        assertThat(transformer.getCollector()).isSameAs(collector1);
        EventCollector collector2 = EventCollector.noop();
        transformer.setCollector(collector2);
        assertThat(transformer.getCollector()).isSameAs(collector2);
    }

    @Test
    void shouldDefaultToEnabled() {
        VtClassFileTransformer transformer = new VtClassFileTransformer(EventCollector.noop());
        assertThat(transformer.isEnabled()).isTrue();
    }

    @Test
    void shouldToggleEnabled() {
        VtClassFileTransformer transformer = new VtClassFileTransformer(EventCollector.noop());
        transformer.setEnabled(false);
        assertThat(transformer.isEnabled()).isFalse();
        transformer.setEnabled(true);
        assertThat(transformer.isEnabled()).isTrue();
    }
}
