package io.github.dlwatching.agent.hook;

import io.github.dlwatching.proto.EventType;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * ASM {@link ClassVisitor} that injects lifecycle event hooks into
 * {@code java.lang.VirtualThread}.
 *
 * <p>Injects three hooks:
 * <ul>
 *   <li><b>CREATED</b> — at the end of every constructor ({@code <init>})</li>
 *   <li><b>STARTED</b> — at the entry of {@code start()}</li>
 *   <li><b>TERMINATED</b> — at every return from {@code run()}</li>
 * </ul>
 *
 * <p>Each hook calls {@link AbstractVtHookVisitor#buildEvent} followed by
 * {@link AbstractVtHookVisitor#getCollector()}.{@code collect(event)}.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class VtLifecycleHookVisitor extends AbstractVtHookVisitor {

    public VtLifecycleHookVisitor(ClassVisitor classVisitor) {
        super(classVisitor);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor,
                                     String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);

        if (name.equals("<init>") && descriptor.startsWith("(")) {
            return new LifecycleHookMv(mv, EventType.CREATED, false, true);
        }
        if (name.equals("start") && descriptor.equals("()V")) {
            return new LifecycleHookMv(mv, EventType.STARTED, true, false);
        }
        if (name.equals("run") && descriptor.equals("()V")) {
            return new LifecycleHookMv(mv, EventType.TERMINATED, false, true);
        }

        return mv;
    }

    /**
     * MethodVisitor that injects a lifecycle hook at method entry or exit.
     */
    private static final class LifecycleHookMv extends MethodVisitor {

        private static final String EVENT_TYPE_OWNER = "io/github/dlwatching/proto/EventType";
        private static final String EVENT_TYPE_DESC = "Lio/github/dlwatching/proto/EventType;";
        private static final String THREAD_OWNER = "java/lang/Thread";
        private static final String HOOK_OWNER =
                "io/github/dlwatching/agent/hook/AbstractVtHookVisitor";
        private static final String COLLECTOR_OWNER =
                "io/github/dlwatching/agent/model/EventCollector";
        private static final String THREAD_EVENT_DESC =
                "Lio/github/dlwatching/proto/ThreadEvent;";

        private final EventType eventType;
        private final boolean hookOnEntry;
        private final boolean hookOnExit;

        LifecycleHookMv(MethodVisitor mv, EventType eventType,
                        boolean hookOnEntry, boolean hookOnExit) {
            super(Opcodes.ASM9, mv);
            this.eventType = eventType;
            this.hookOnEntry = hookOnEntry;
            this.hookOnExit = hookOnExit;
        }

        @Override
        public void visitCode() {
            super.visitCode();
            if (hookOnEntry) {
                injectHook();
            }
        }

        @Override
        public void visitInsn(int opcode) {
            if (hookOnExit && opcode == Opcodes.RETURN) {
                injectHook();
            }
            super.visitInsn(opcode);
        }

        private void injectHook() {
            // EventType.CREATED / STARTED / TERMINATED
            mv.visitFieldInsn(Opcodes.GETSTATIC, EVENT_TYPE_OWNER,
                    eventType.name(), EVENT_TYPE_DESC);

            // this.threadId()
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, THREAD_OWNER,
                    "threadId", "()J", false);

            // this.getName()
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, THREAD_OWNER,
                    "getName", "()Ljava/lang/String;", false);

            // Thread.currentThread()
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, THREAD_OWNER,
                    "currentThread", "()Ljava/lang/Thread;", false);
            // .getName()
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, THREAD_OWNER,
                    "getName", "()Ljava/lang/String;", false);

            // AbstractVtHookVisitor.buildEvent(EventType, long, String, String)
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, HOOK_OWNER,
                    "buildEvent",
                    "(" + EVENT_TYPE_DESC + "JLjava/lang/String;Ljava/lang/String;)"
                            + THREAD_EVENT_DESC,
                    false);

            // AbstractVtHookVisitor.getCollector()
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, HOOK_OWNER,
                    "getCollector", "()L" + COLLECTOR_OWNER + ";", false);

            // stack is: [ThreadEvent, EventCollector]
            // INVOKEINTERFACE needs: [EventCollector, ThreadEvent]
            mv.visitInsn(Opcodes.SWAP);

            // EventCollector.collect(ThreadEvent)
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, COLLECTOR_OWNER,
                    "collect", "(" + THREAD_EVENT_DESC + ")V", true);
        }
    }
}
