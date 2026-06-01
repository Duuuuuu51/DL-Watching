package io.github.dlwatching.agent.hook;

import org.objectweb.asm.ClassVisitor;

/**
 * Stub implementation. Full scheduling hooks (park/unpark/mount/unmount) will be
 * implemented in M4.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt;
 */
public class VtSchedulingHookVisitor extends AbstractVtHookVisitor {

    public VtSchedulingHookVisitor(ClassVisitor classVisitor) {
        super(classVisitor);
    }
}
