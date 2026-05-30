package io.github.dlwatching.agent.hook;

import org.objectweb.asm.ClassVisitor;

/**
 * Stub implementation. Full lifecycle hooks (create/start/terminate) will be
 * implemented in M3.
 */
public class VtLifecycleHookVisitor extends AbstractVtHookVisitor {

    public VtLifecycleHookVisitor(ClassVisitor classVisitor) {
        super(classVisitor);
    }
}
