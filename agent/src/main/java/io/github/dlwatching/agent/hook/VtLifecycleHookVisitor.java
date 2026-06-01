package io.github.dlwatching.agent.hook;

import org.objectweb.asm.ClassVisitor;

/**
 * Stub implementation. Full lifecycle hooks (create/start/terminate) will be
 * implemented in M3.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class VtLifecycleHookVisitor extends AbstractVtHookVisitor {

    public VtLifecycleHookVisitor(ClassVisitor classVisitor) {
        super(classVisitor);
    }
}
