package ru.mail.polis.rubtsov;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Part of storage located at disk.
 */

public class SSTable {
    private ByteBuffer records;
    private LongBuffer offsets;
    private long recordsAmount;

    /**
     * Creates a new representation of data file.
     *
     * @param tableFile file with data
     */

    public SSTable(final File tableFile) throws IOException,
            IllegalArgumentException, IndexOutOfBoundsException {
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(
                tableFile.toPath(), StandardOpenOption.READ)) {
            final ByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, tableFile.length()).order(ByteOrder.BIG_ENDIAN);
            recordsAmount = mappedByteBuffer.getLong(mappedByteBuffer.limit() - Long.BYTES);
            offsets = mappedByteBuffer.duplicate()
                    .position((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 1)))
                    .limit(mappedByteBuffer.limit() - Long.BYTES).slice().asLongBuffer();
            records = mappedByteBuffer.duplicate()
                    .limit((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 1)))
                    .slice().asReadOnlyBuffer();
        }
    }

    private ByteBuffer getRecord(final long index) {
        final long offset = offsets.get((int) index);
        long recordLimit;
        if (index == recordsAmount - 1) {
            recordLimit = records.limit();
        } else {
            recordLimit = offsets.get((int) index + 1);
        }
        return records.duplicate().position((int) offset).limit((int) recordLimit).slice().asReadOnlyBuffer();
    }

    private ByteBuffer getKey(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        final int keySize = rec.getInt();
        rec.limit(Integer.BYTES + keySize).slice().asReadOnlyBuffer();
        return rec;
    }

    private long getTimeStamp(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        rec.position(Integer.BYTES + rec.getInt());
        return rec.getLong();
    }

    private ByteBuffer getValue(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        final int keySize = rec.getInt();
        return rec.position(Integer.BYTES + keySize + Long.BYTES * 2).slice().asReadOnlyBuffer();
    }

    private Item getItem(final long pos) {
        final ByteBuffer rec = getRecord(pos);
        final ByteBuffer key = getKey(rec);
        final long timeStamp = getTimeStamp(rec);
        ByteBuffer value;
        if (timeStamp < 0) {
            value = Item.TOMBSTONE;
        } else {
            value = getValue(rec);
        }
        return Item.of(key, value, timeStamp);
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

    /**
     * Returns an iterator over the elements in this table.
     *
     * @param from the key from which to start the iteration.
     * @return iterator
     */

    public Iterator<Item> iterator(final ByteBuffer from) {
        return new Iterator<Item>() {
            long pos = getPosition(from);

            @Override
            public boolean hasNext() {
                return pos < recordsAmount;
            }

            @Override
            public Item next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more elements");
                }
                final Item item = getItem(pos);
                pos++;
                return item;
            }
        };
    }

    /**
     * Tests new SSTable for validity. Throws an exceptions if file is corrupted.
     *
     * @throws IllegalArgumentException error when setting a position that not really exists
     * @throws IndexOutOfBoundsException database structure corrupted
     */

    public void testTable() throws IllegalArgumentException, IndexOutOfBoundsException {
        final Iterator<Item> itemIterator = iterator(ByteBuffer.allocate(0));
        while (itemIterator.hasNext()) {
            itemIterator.next();
        }
    }
}
