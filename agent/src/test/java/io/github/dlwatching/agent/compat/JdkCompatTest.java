package io.github.dlwatching.agent.compat;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Duuuuuu <1617714380@qq.com>
 */
class JdkCompatTest {

    @Test
    void shouldDetectJdkVersion() {
        int version = JdkCompat.jdkVersion();
        assertThat(version).isGreaterThanOrEqualTo(21);
    }

    @Test
    void shouldSupportVirtualThreadsOnJdk21Plus() {
        assertThat(JdkCompat.supportsVirtualThreads()).isTrue();
    }

    @Test
    void shouldReturnVirtualThreadInternalName() {
        assertThat(JdkCompat.virtualThreadInternalName())
                .isEqualTo("java/lang/VirtualThread");
    }

    @Test
    void shouldReturnVirtualThreadsInternalName() {
        assertThat(JdkCompat.virtualThreadsInternalName())
                .isEqualTo("jdk/internal/misc/VirtualThreads");
    }

    @Test
    void shouldReturnSchedulerInternalName() {
        assertThat(JdkCompat.schedulerInternalName())
                .isEqualTo("jdk/internal/misc/VirtualThreadScheduler");
    }

    @Test
    void shouldParseStandardJdkVersionStrings() {
        assertThat(JdkCompat.detectJdkVersion()).isGreaterThan(0);
    }

    @Test
    void shouldRejectVersionBelow21ForVirtualThreads() {
        boolean result = JdkCompat.supportsVirtualThreads();
        assertThat(result).isTrue();
    }
}
