package ru.mail.polis.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Comparator.comparing;

public final class Item implements Comparable<Item> {

    static final Comparator<Item> COMPARATOR = comparing(Item::getKey)
            .thenComparing(comparing(Item::getTimeStampAbs).reversed());
    private static long millis;

    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    private static AtomicInteger additionalTime = new AtomicInteger();
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timeStamp;

    private Item(final ByteBuffer key, final ByteBuffer value, final long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public static Item of(final ByteBuffer key, final ByteBuffer value) {
        return new Item(key.duplicate(), value.duplicate(), getCurrentTime());
    }

    public static Item of(final ByteBuffer key, final ByteBuffer value, final long timeStamp) {
        return new Item(key.duplicate(), value.duplicate(), timeStamp);
    }

    public static Item removed(final ByteBuffer key) {
        return new Item(key.duplicate(), TOMBSTONE, -getCurrentTime());
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    private static long getCurrentTime() {
        final long systemCurrentTime = System.currentTimeMillis();
        if (millis != systemCurrentTime) {
            millis = systemCurrentTime;
            additionalTime.set(0);
        }
        return millis * 1_000_000 + additionalTime.getAndIncrement();
    }

    public boolean isRemoved() {
        return getTimeStamp() < 0;
    }

    @Override
    public int compareTo(@NotNull final Item o) {
        return COMPARATOR.compare(this, o);
    }

    /**
     * Returns size of current item in serialized form in bytes.
     *
     * @return size of item in bytes
     */

    public long getSizeInBytes() {
        final int keyRem = key.remaining();
        final int valRem = value.remaining();
        final int valLen = value.remaining() == 0 ? 0 : Long.BYTES;
        return Integer.BYTES
                + keyRem
                + Long.BYTES
                + valRem
                + valLen;
    }

    public long getTimeStampAbs() {
        return Math.abs(timeStamp);
    }

}
