package ru.mail.polis.rubtsov;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Part of storage located in RAM.
 */

public class MemTable implements Closeable {

    private final long flushThresholdInBytes;

    private final SortedMap<ByteBuffer, Item> data;
    private long sizeInBytes;
    private final File ssTablesDir;

    /**
     * Creates a new RAM-storage.
     *
     * @param ssTablesDir folder which MemTable will flush the data
     * @param heapSizeInBytes given JVM max heap size
     */

    public MemTable(final File ssTablesDir, final long heapSizeInBytes) {
        data = new TreeMap<>();
        this.ssTablesDir = ssTablesDir;
        flushThresholdInBytes = heapSizeInBytes / 8;
    }

    public Iterator<Item> iterator(final ByteBuffer from) {
        return data.tailMap(from).values().iterator();
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */

    public void upsert(final ByteBuffer key, final ByteBuffer value) {
        final Item val = Item.of(key, value);
        if (isFlushNeeded(val)) {
            flush();
        }
        final Item previousItem = data.put(key, val);
        if (previousItem == null) {
            sizeInBytes += val.getSizeInBytes();
        } else {
            sizeInBytes += -previousItem.getSizeInBytes() + val.getSizeInBytes();
        }
    }

    /**
     * Removes the mapping for this key from this table if present.
     *
     * @param key that should be removed
     */

    public void remove(final ByteBuffer key) {
        final Item dead = Item.removed(key);
        if (isFlushNeeded(dead)) {
            flush();
        }
        final Item previousItem = data.put(key, dead);
        if (previousItem == null) {
            sizeInBytes += dead.getSizeInBytes();
        } else {
            sizeInBytes += -previousItem.getSizeInBytes() + dead.getSizeInBytes();
        }
    }

    private boolean isFlushNeeded(final Item item) {
        return (sizeInBytes + item.getSizeInBytes()) > flushThresholdInBytes;
    }

    private void flush() {
        final ByteBuffer offsets = ByteBuffer.allocate((data.size() + 1) * Long.BYTES);
        long offset = 0;
        offsets.putLong(offset);
        final String fileName = System.currentTimeMillis() + ".tmp";
        final String fileNameComplete = fileName.substring(0, fileName.length() - 3) + "dat";
        final Path path = ssTablesDir.toPath().resolve(Paths.get(fileName));
        final Path pathComplete = ssTablesDir.toPath().resolve(Paths.get(fileNameComplete));
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            for (final Item v :
                    data.values()) {
                final ByteBuffer key = v.getKey();
                final ByteBuffer value = v.getValue();
                final ByteBuffer row = ByteBuffer.allocate((int) v.getSizeInBytes());
                row.putInt(key.remaining()).put(key.duplicate()).putLong(v.getTimeStamp());
                if (!v.isRemoved()) {
                    row.putLong(value.remaining()).put(value.duplicate());
                }
                offset += v.getSizeInBytes();
                offsets.putLong(offset);
                row.flip();
                fileChannel.write(row);
            }
            offsets.position(offsets.limit() - Long.BYTES).putLong((long) data.size());
            offsets.flip();
            fileChannel.write(offsets);
            Files.move(path, pathComplete, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        data.clear();
        sizeInBytes = 0;
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
