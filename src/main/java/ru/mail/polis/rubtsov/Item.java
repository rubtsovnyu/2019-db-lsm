package ru.mail.polis.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static java.util.Comparator.comparing;

public final class Item implements Comparable<Item> {
    static final Comparator<Item> COMPARATOR = comparing(Item::getKey)
            .thenComparing(comparing(Item::getTimeStamp).reversed());
    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final long NO_TTL = -1;

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timeStamp;
    private final long timeToLive;
    private final boolean removed;

    private Item(final ByteBuffer key, final ByteBuffer value, final long timeStamp, final boolean removed) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
        this.removed = removed;
        this.timeToLive = NO_TTL;
    }

    private Item(final ByteBuffer key, final ByteBuffer value, final long timeStamp,
                 final boolean removed, final long timeToLive) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
        this.removed = removed;
        this.timeToLive = timeToLive;
    }

    public static Item of(final ByteBuffer key, final ByteBuffer value) {
        return new Item(key.duplicate(), value.duplicate(), TimeUtils.getCurrentTime(), false);
    }

    static Item ofTTL(final ByteBuffer key, final ByteBuffer value, final long timeToLive) {
        return new Item(key.duplicate(), value.duplicate(),
                TimeUtils.getCurrentTime(), false, timeToLive);
    }

    static Item ofTTL(final ByteBuffer key, final ByteBuffer value,
                      final long timeStamp, final long timeToLive) {
        return new Item(key.duplicate(), value.duplicate(), timeStamp, false, timeToLive);
    }

    static Item removed(final ByteBuffer key) {
        return removed(key, TimeUtils.getCurrentTime(), NO_TTL);
    }

    static Item removed(final ByteBuffer key, final long timeStamp, final long timeToLive) {
        return new Item(key.duplicate(), TOMBSTONE, timeStamp, true, timeToLive);
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
        return removed || hasTTL() && isExpired();
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
        return getSizeInBytes(isRemoved());
    }

    long getSizeInBytes(final boolean removed) {
        final int keyRem = key.remaining();
        final int valRem = removed ? 0 : value.remaining();
        final int valLen = removed ? 0 : Long.BYTES;
        return Integer.BYTES
                + keyRem
                + Long.BYTES
                + valRem
                + valLen
                + Long.BYTES;
    }
}
