package io.github.dlwatching.agent.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded concurrent ring buffer with FIFO overwrite semantics.
 * Uses atomic head/tail cursors for lock-free offer and poll operations.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class RingBuffer<T> {

    private static final int DEFAULT_CAPACITY = 10000;

    private final Object[] buffer;
    private final int capacity;
    private final AtomicLong head = new AtomicLong(0);
    private final AtomicLong tail = new AtomicLong(0);
    private final AtomicLong drops = new AtomicLong(0);

    public RingBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public RingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new Object[capacity];
    }

    /**
     * Offer an item to the buffer. If the buffer is full, the oldest item
     * is dropped (FIFO overwrite) and this method returns false.
     *
     * @param item the item to add; must not be null
     * @return true if no item was dropped, false if an older item was discarded
     */
    public boolean offer(T item) {
        if (item == null) {
            throw new NullPointerException("Item must not be null");
        }
        long t = tail.getAndIncrement();
        long h = head.get();
        boolean dropped = false;
        if (t - h >= capacity) {
            head.getAndIncrement();
            drops.incrementAndGet();
            dropped = true;
        }
        int index = (int) (t % capacity);
        buffer[index] = item;
        return !dropped;
    }

    @SuppressWarnings("unchecked")
    public T poll() {
        while (true) {
            long h = head.get();
            long t = tail.get();
            if (h >= t) {
                return null;
            }
            if (head.compareAndSet(h, h + 1)) {
                int index = (int) (h % capacity);
                T item = (T) buffer[index];
                buffer[index] = null;
                return item;
            }
        }
    }

    public int size() {
        long diff = tail.get() - head.get();
        return (int) Math.max(0, Math.min(diff, Integer.MAX_VALUE));
    }

    public int capacity() {
        return capacity;
    }

    public long drops() {
        return drops.get();
    }

    public void clear() {
        while (poll() != null) {
            // drain
        }
    }
}
