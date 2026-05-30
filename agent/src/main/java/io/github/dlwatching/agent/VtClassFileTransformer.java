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

public class VtClassFileTransformer implements ClassFileTransformer {

    static final Set<String> TARGET_INTERNAL_NAMES = Set.of(
            JdkCompat.virtualThreadInternalName()
    );

    private volatile boolean enabled = true;
    private volatile EventCollector collector;

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

    byte[] transformClass(byte[] classBytes) {
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);

        ClassVisitor visitor = new VtLifecycleHookVisitor(cw);
        visitor = new VtSchedulingHookVisitor(visitor);

        cr.accept(visitor, ClassReader.EXPAND_FRAMES);
        return cw.toByteArray();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public EventCollector getCollector() {
        return collector;
    }

    public void setCollector(EventCollector collector) {
        this.collector = collector != null ? collector : EventCollector.noop();
        AbstractVtHookVisitor.setCollector(this.collector);
    }
}
