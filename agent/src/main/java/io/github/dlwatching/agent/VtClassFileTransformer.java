package io.github.dlwatching.agent;

import io.github.dlwatching.agent.compat.JdkCompat;
import io.github.dlwatching.agent.hook.AbstractVtHookVisitor;
import io.github.dlwatching.agent.hook.VtLifecycleHookVisitor;
import io.github.dlwatching.agent.hook.VtSchedulingHookVisitor;
import io.github.dlwatching.agent.model.EventCollector;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.Set;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

/**
 * {@link ClassFileTransformer} that instruments {@code java.lang.VirtualThread}
 * with ASM-based lifecycle and scheduling hook visitors.
 *
 * <p>Only classes matching the target internal names are transformed.
 * All other classes pass through unmodified.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class VtClassFileTransformer implements ClassFileTransformer {

    /**
     * Set of internal class names that this transformer will instrument.
     */
    static final Set<String> TARGET_INTERNAL_NAMES = Set.of(
            JdkCompat.virtualThreadInternalName()
    );

    private volatile boolean enabled = true;
    private volatile EventCollector collector;

    /**
     * Creates a transformer with the given event collector.
     *
     * @param collector the collector to wire into hook visitors
     */
    public VtClassFileTransformer(EventCollector collector) {
        this.collector = collector != null ? collector : EventCollector.noop();
    }

    @Override
    public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) throws IllegalClassFormatException {

        if (!enabled || className == null) {
            return null;
        }

        if (!TARGET_INTERNAL_NAMES.contains(className)) {
            return null;
        }

        try {
            return transformClass(classfileBuffer);
        } catch (Exception e) {
            System.err.println("[DL-Watching Agent] Failed to transform class: "
                    + className + " - " + e.getMessage());
            return null;
        }
    }

    /**
     * Performs the actual ASM transformation on the class bytes.
     */
    byte[] transformClass(byte[] classBytes) {
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);

        ClassVisitor visitor = new VtLifecycleHookVisitor(cw);
        visitor = new VtSchedulingHookVisitor(visitor);

        cr.accept(visitor, ClassReader.EXPAND_FRAMES);
        return cw.toByteArray();
    }

    /**
     * Returns whether this transformer is currently enabled.
     *
     * @return {@code true} if transformation is active
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables class transformation.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the event collector used by this transformer.
     *
     * @return the event collector (never {@code null})
     */
    public EventCollector getCollector() {
        return collector;
    }

    /**
     * Replaces the event collector. Existing events in the old collector
     * are not transferred.
     *
     * @param collector the new event collector
     */
    public void setCollector(EventCollector collector) {
        this.collector = collector != null ? collector : EventCollector.noop();
        AbstractVtHookVisitor.setCollector(this.collector);
    }
}
