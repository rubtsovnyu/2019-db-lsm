package ru.mail.polis.rubtsov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Rec implements Comparable<Rec> {
    ByteBuffer key;
    long timeStamp;
    long offset;

    @Override
    public int compareTo(@NotNull Rec o) {
        return Long.compare(this.offset, o.getOffset());
    }

    private Rec() {}

    private Rec(ByteBuffer key, long timeStamp, long offset) {
        this.key = key;
        this.timeStamp = timeStamp;
        this.offset = offset;
    }

    public static Rec of(ByteBuffer key, long timeStamp, long offset) {
        return new Rec(key.duplicate(), timeStamp, offset);
    }

    ByteBuffer getKey() {
        return key;
    }

    long getTimeStamp() {
        return timeStamp;
    }

    long getOffset() {
        return offset;
    }

    long getSize() {
        return Integer.BYTES + key.remaining() + Long.BYTES * 2;
    }
}