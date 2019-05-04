package ru.mail.polis.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Item implements Comparable<Item> {

    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final Comparator<Item> COMPARATOR = Comparator.comparing(Item::getKey).thenComparing(Item::getValue);
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timeStamp;

    private Item(ByteBuffer key, ByteBuffer value, long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public static Item of(ByteBuffer key, ByteBuffer value) {
        return new Item(key, value, System.currentTimeMillis());
    }

    public static Item of(ByteBuffer key, ByteBuffer value, long timeStamp) {
        return new Item(key, value, timeStamp);
    }

    public static Item removed(ByteBuffer key) {
        return new Item(key, TOMBSTONE, -System.currentTimeMillis());
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

}
