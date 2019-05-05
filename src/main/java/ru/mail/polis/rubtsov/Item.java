package ru.mail.polis.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Comparator.comparing;

public class Item implements Comparable<Item> {

    static final Comparator<Item> COMPARATOR = comparing(Item::getKey).thenComparing(comparing(Item::getTimeStampAbs).reversed());
    private static long millis = 0;

    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    private static AtomicInteger additionalTime = new AtomicInteger();
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timeStamp;

    private Item(ByteBuffer key, ByteBuffer value, long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public static Item of(ByteBuffer key, ByteBuffer value) {
        return new Item(key, value, getCurrentTime());
    }

    public static Item of(ByteBuffer key, ByteBuffer value, long timeStamp) {
        return new Item(key, value, timeStamp);
    }

    public static Item removed(ByteBuffer key) {
        return new Item(key, TOMBSTONE, -getCurrentTime());
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
        long systemCurrentTime = System.currentTimeMillis();
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
    public int compareTo(@NotNull Item o) {
        return COMPARATOR.compare(this, o);
    }

    public long getSizeInBytes() {
        int keyRem = key.remaining();
        int valRem = value.remaining();
        int valLen = value.remaining() != 0 ? Long.BYTES : 0;
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
