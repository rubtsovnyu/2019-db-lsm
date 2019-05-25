package ru.mail.polis.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static java.util.Comparator.comparing;

public final class Item implements Comparable<Item> {
    static final Comparator<Item> COMPARATOR = comparing(Item::getKey)
            .thenComparing(comparing(Item::getTimeStampAbs).reversed());
    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final long NO_TTL = -1;

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timeStamp;
    private final long timeToLive;

    private Item(final ByteBuffer key, final ByteBuffer value, final long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
        this.timeToLive = NO_TTL;
    }

    private Item(final ByteBuffer key, final ByteBuffer value, final long timeStamp, final long timeToLive) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
        this.timeToLive = timeToLive;
    }

    public static Item of(final ByteBuffer key, final ByteBuffer value) {
        return new Item(key.duplicate(), value.duplicate(), TimeUtils.getCurrentTime());
    }

    static Item ofTTL(final ByteBuffer key, final ByteBuffer value, final long timeToLive) {
        return new Item(key.duplicate(), value.duplicate(), TimeUtils.getCurrentTime(), timeToLive);
    }

    static Item ofTTL(final ByteBuffer key, final ByteBuffer value, final long timeStamp, final long timeToLive) {
        return new Item(key.duplicate(), value.duplicate(), timeStamp, timeToLive);
    }

    static Item removed(final ByteBuffer key) {
        return new Item(key.duplicate(), TOMBSTONE, -TimeUtils.getCurrentTime());
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    long getTimeStamp() {
        return timeStamp;
    }

    long getTimeToLive() {
        return timeToLive;
    }

    boolean isRemoved() {
        return timeStamp < 0 || hasTTL() && isExpired();
    }

    private boolean hasTTL() {
        return timeToLive > NO_TTL;
    }

    private boolean isExpired() {
        return System.currentTimeMillis() > (timeStamp / 1_000_000 + timeToLive);
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

    long getSizeInBytes() {
        final int keyRem = key.remaining();
        final boolean isRemoved = isRemoved();
        final int valRem = isRemoved ? 0 : value.remaining();
        final int valLen = isRemoved ? 0 : Long.BYTES;
        return Integer.BYTES
                + keyRem
                + Long.BYTES
                + valRem
                + valLen
                + Long.BYTES;
    }

    long getTimeStampAbs() {
        return Math.abs(timeStamp);
    }
}
