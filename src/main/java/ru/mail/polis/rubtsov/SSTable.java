package ru.mail.polis.rubtsov;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

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

    public static Path writeNewTable(Iterator<Item> items, File ssTablesDir) throws IOException {
        List<Long> offsets = new ArrayList<>();
        long offset = 0;
        offsets.add(offset);
        final String fileName = UUID.randomUUID().toString() + ".tmp";
        final String fileNameComplete = fileName.substring(0, fileName.length() - 3) + "dat";
        final Path path = ssTablesDir.toPath().resolve(Paths.get(fileName));
        final Path pathComplete = ssTablesDir.toPath().resolve(Paths.get(fileNameComplete));
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            while (items.hasNext()) {
                Item item = items.next();
                final ByteBuffer key = item.getKey();
                final ByteBuffer value = item.getValue();
                final ByteBuffer row = ByteBuffer.allocate((int) item.getSizeInBytes());
                row.putInt(key.remaining()).put(key.duplicate()).putLong(item.getTimeStamp());
                if (!item.isRemoved()) {
                    row.putLong(value.remaining()).put(value.duplicate());
                }
                offset += item.getSizeInBytes();
                offsets.add(offset);
                row.flip();
                fileChannel.write(row);
            }
            int offsetsCount = offsets.size();
            offsets.set(offsetsCount - 1, (long) offsetsCount - 1);
            final ByteBuffer offsetsByteBuffer = ByteBuffer.allocate(offsetsCount * Long.BYTES);
            for (Long i :
                    offsets) {
                offsetsByteBuffer.putLong(i);
            }
            offsetsByteBuffer.flip();
            fileChannel.write(offsetsByteBuffer);
            Files.move(path, pathComplete, StandardCopyOption.ATOMIC_MOVE);
        }
        return pathComplete;
    }

    /**
     * Tests new SSTable for validity. Throws an exceptions if file is corrupted.
     *
     * @throws IllegalArgumentException  error when setting a position that not really exists
     * @throws IndexOutOfBoundsException database structure corrupted
     */

    public void testTable() throws IllegalArgumentException, IndexOutOfBoundsException {
        final Iterator<Item> itemIterator = iterator(ByteBuffer.allocate(0));
        while (itemIterator.hasNext()) {
            itemIterator.next();
        }
    }

    /**
     * Returns an iterator over the elements in this table.
     *
     * @param from the key from which to start the iteration.
     * @return iterator
     */

    public Iterator<Item> iterator(final ByteBuffer from) {
        return new Iterator<>() {
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
}
