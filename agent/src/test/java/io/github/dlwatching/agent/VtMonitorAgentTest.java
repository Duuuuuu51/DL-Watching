package io.github.dlwatching.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.agent.model.EventCollector;
import org.junit.jupiter.api.Test;

class VtMonitorAgentTest {

    @Test
    void shouldHaveVersionConstant() {
        assertThat(VtMonitorAgent.VERSION).isNotNull().isEqualTo("0.5.0-SNAPSHOT");
    }

    @Test
    void shouldReturnNoopCollectorByDefault() {
        EventCollector collector = VtMonitorAgent.getCollector();
        assertThat(collector).isNotNull();
    }

    @Test
    void shouldReturnTransformerInstance() {
        VtClassFileTransformer t = VtMonitorAgent.getTransformer();
        assertThat(t).isNotNull();
        assertThat(t.isEnabled()).isTrue();
    }

    @Test
    void shouldSupportCollectorSwap() {
        EventCollector original = VtMonitorAgent.getCollector();
        EventCollector newCollector = EventCollector.noop();
        VtMonitorAgent.getTransformer().setCollector(newCollector);
        try {
            assertThat(VtMonitorAgent.getCollector()).isSameAs(newCollector);
        } finally {
            VtMonitorAgent.getTransformer().setCollector(original);
        }
    }

    @Test
    void versionShouldFollowSemver() {
        assertThat(VtMonitorAgent.VERSION).matches("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?");
    }

    @Test
    void premainShouldNotThrow() {
        assertThat(VtMonitorAgent.getTransformer()).isNotNull();
    }
}
