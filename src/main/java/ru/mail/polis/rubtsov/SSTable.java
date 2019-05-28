package ru.mail.polis.rubtsov;

import com.google.common.base.Preconditions;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.NoSuchElementException;

/**
 * Part of storage located at disk.
 */
final class SSTable {
    private static final String TEMP_FILE_EXTENSTION = ".tmp";
    static final String VALID_FILE_EXTENSTION = ".dat";

    private final ByteBuffer records;
    private final LongBuffer offsets;
    private final long recordsAmount;
    private final File tableFile;

    /**
     * Creates a new representation of data file.
     *
     * @param tableFile file with data
     * @throws IllegalArgumentException if file corrupted
     */
    SSTable(final File tableFile) throws IOException {
        this.tableFile = tableFile;
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(
                tableFile.toPath(), StandardOpenOption.READ)) {
            Preconditions.checkArgument(fileChannel.size() >= Long.BYTES);
            final ByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, tableFile.length()).order(ByteOrder.BIG_ENDIAN);
            Preconditions.checkArgument(mappedByteBuffer.limit() < Integer.MAX_VALUE);
            recordsAmount = mappedByteBuffer.getLong(mappedByteBuffer.limit() - Long.BYTES);
            Preconditions.checkArgument(mappedByteBuffer.limit() > recordsAmount * 21);
            offsets = mappedByteBuffer.duplicate()
                    .position((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 2)))
                    .limit(mappedByteBuffer.limit() - Long.BYTES)
                    .slice()
                    .asLongBuffer();
            Preconditions.checkArgument(offsets.limit() == recordsAmount + 1);
            records = mappedByteBuffer.duplicate()
                    .limit((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 2)))
                    .slice()
                    .asReadOnlyBuffer();
        }
    }

    /**
     * Writes new SSTable on disk.
     * Format:
     * { [key size][key][timestamp] (if value exists [value size][value]) [time to live] } * records amount
     * at the end of file - [array of longs that contains offsets][offsets number]
     *
     * @param items iterator of data that should be written
     * @param ssTablesDir data files directory
     * @return path of new file
     * @throws IOException if something went wrong during writing
     */
    static Path writeNewTable(final Iterator<Item> items, final File ssTablesDir) throws IOException {
        final List<Long> offsets = new ArrayList<>();
        long offset = 0;
        offsets.add(offset);
        final String uuid = UUID.randomUUID().toString();
        final String fileName = uuid + TEMP_FILE_EXTENSTION;
        final String fileNameComplete = uuid + VALID_FILE_EXTENSTION;
        final Path path = ssTablesDir.toPath().resolve(Paths.get(fileName));
        final Path pathComplete = ssTablesDir.toPath().resolve(Paths.get(fileNameComplete));
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            while (items.hasNext()) {
                final Item item = items.next();
                final ByteBuffer key = item.getKey();
                final ByteBuffer value = item.getValue();
                final boolean removed = item.isRemoved();
                final int itemSize = (int) item.getSizeInBytes(removed);
                final ByteBuffer row = ByteBuffer.allocate(itemSize);
                row.putInt(key.remaining()).put(key.duplicate());
                if (removed) {
                    row.putLong(-item.getTimeStamp());
                } else {
                    row.putLong(item.getTimeStamp()).putLong(value.remaining()).put(value.duplicate());
                }
                row.putLong(item.getTimeToLive());
                offset += itemSize;
                offsets.add(offset);
                row.flip();
                fileChannel.write(row);
            }
            final ByteBuffer offsetsByteBuffer = ByteBuffer.allocate((offsets.size() + 1) * Long.BYTES);
            for (final Long i : offsets) {
                offsetsByteBuffer.putLong(i);
            }
            offsetsByteBuffer.putLong(offsets.size() - 1);
            offsetsByteBuffer.flip();
            fileChannel.write(offsetsByteBuffer);
            Files.move(path, pathComplete, StandardCopyOption.ATOMIC_MOVE);
        }
        return pathComplete;
    }

    private ByteBuffer getRecord(final long index) {
        final long offset = offsets.get((int) index);
        return records.duplicate()
                .position((int) offset)
                .limit((int) offsets.get((int) index + 1))
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

    private ByteBuffer getValue(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        final int keySize = rec.getInt();
        return rec.position(Integer.BYTES + keySize + Long.BYTES * 2)
                .limit(rec.limit() - Long.BYTES)
                .slice()
                .asReadOnlyBuffer();
    }

    private long getTimeStamp(final ByteBuffer record) {
        final ByteBuffer rec = record.duplicate();
        rec.position(Integer.BYTES + rec.getInt());
        return rec.getLong();
    }

    private long getTimeToLive(final ByteBuffer record) {
        return record.getLong(record.limit() - Long.BYTES);
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

    private Item getItem(final long pos) {
        final ByteBuffer rec = getRecord(pos);
        final ByteBuffer key = getKey(rec);
        final long timeStamp = getTimeStamp(rec);
        final boolean isRemoved = timeStamp < 0;
        final long timeToLive = getTimeToLive(rec);
        if (isRemoved) {
            return Item.removed(key, Math.abs(timeStamp), timeToLive);
        } else {
            return Item.ofTTL(key, getValue(rec), Math.abs(timeStamp), timeToLive);
        }
    }

    /**
     * Returns file this SSTable associated with.
     *
     * @return file
     */
    File getTableFile() {
        return tableFile;
    }

    /**
     * Returns an iterator over the elements in this table.
     *
     * @param from the key from which to start the iteration.
     * @return iterator
     */
    Iterator<Item> iterator(final ByteBuffer from) {
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
