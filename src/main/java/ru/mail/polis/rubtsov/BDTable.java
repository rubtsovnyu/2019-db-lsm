package ru.mail.polis.rubtsov;

import com.google.common.base.Preconditions;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class BDTable {
    static final String HUGE_VALUE_FILE_EXTENSION = ".hdat";
    static final String HUGE_VALUE_INDEX_FILE_EXTENSION = ".hidx";

    private final Path tableDataFile;
    private final Path tableIndexFile;
    private ByteBuffer indexBuffer;
    private LongBuffer offsets;
    private long recordsAmount;

    public BDTable(final Path tableDataFile, final Path tableIndexFile) throws IOException {
        this.tableDataFile = tableDataFile;
        this.tableIndexFile = tableIndexFile;
        Preconditions.checkArgument(Files.isReadable(tableDataFile));
        Preconditions.checkArgument(Files.isReadable(tableIndexFile));
        try (FileChannel fileChannel = FileChannel.open(
                tableIndexFile, StandardOpenOption.READ)) {
            final ByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, fileChannel.size()).order(ByteOrder.BIG_ENDIAN);
            recordsAmount = mappedByteBuffer.getLong(mappedByteBuffer.limit() - Long.BYTES);
            Preconditions.checkArgument(mappedByteBuffer.limit() > recordsAmount * 21);
            offsets = mappedByteBuffer.duplicate()
                    .position((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 2)))
                    .limit(mappedByteBuffer.limit() - Long.BYTES)
                    .slice()
                    .asLongBuffer();
            indexBuffer = mappedByteBuffer.duplicate()
                    .limit((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 2)))
                    .slice()
                    .asReadOnlyBuffer();
        }
    }



//    InputStream get(final ByteBuffer key) throws IOException {
//        long offset = data.get(key).getOffset();
//        FileInputStream value = new FileInputStream(tableDataFile.toFile());
//        value.skip(offset);
//        value.mark();
//        return value;
//    }

    private ByteBuffer getRecord(final long index) {
        final long offset = indexBuffer.get((int) index);
        return indexBuffer.duplicate()
                .position((int) offset)
                .limit((int) indexBuffer.get((int) index + 1))
                .slice()
                .asReadOnlyBuffer();
    }

    private ByteBuffer getKey(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        final int keySize = rec.getInt();
        return rec.limit(Integer.BYTES + keySize)
                .slice()
                .asReadOnlyBuffer();
    }

    private long getTimeStamp(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        rec.position(Integer.BYTES + rec.getInt());
        return rec.getLong();
    }

    private long getPosition(final ByteBuffer key) {
        long left = 0;
        long right = recordsAmount - 1;
        while (left <= right) {
            final long mid = left + (right - left) / 2;
            final int compare = getKey(getRecord(mid)).compareTo(key);
            if (compare > 0) {
                right = mid - 1;
            } else if (compare < 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }



}
