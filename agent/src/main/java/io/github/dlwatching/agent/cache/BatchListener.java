package io.github.dlwatching.agent.cache;

import io.github.dlwatching.proto.EventBatch;

/**
 * Callback interface for receiving assembled event batches.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
@FunctionalInterface
public interface BatchListener {
    void onBatch(EventBatch batch);
}
