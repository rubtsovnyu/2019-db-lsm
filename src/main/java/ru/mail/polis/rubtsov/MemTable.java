package ru.mail.polis.rubtsov;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Closeable {

    private final long FLUSH_THRESHOLD_BYTES;

    private SortedMap<ByteBuffer, Item> data;
    private long sizeInBytes;
    private File ssTablesDir;

    public MemTable(File ssTablesDir, long MAX_HEAP) {
        data = new TreeMap<>();
        this.ssTablesDir = ssTablesDir;
        FLUSH_THRESHOLD_BYTES = MAX_HEAP / 8;
    }

    public Iterator<Item> iterator(ByteBuffer from) {
        return data.tailMap(from).values().iterator();
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        Item val = Item.of(key, value);
        if (isFlushNeeded(val)) {
            flush();
        }
        Item previousItem = data.put(key, val);
        if (previousItem != null) {
            sizeInBytes += -previousItem.getSizeInBytes() + val.getSizeInBytes();
        } else {
            sizeInBytes += val.getSizeInBytes();
        }
    }

    public void remove(ByteBuffer key) {
        Item dead = Item.removed(key);
        if (isFlushNeeded(dead)) {
            flush();
        }
        Item previousItem = data.put(key, dead);
        if (previousItem != null) {
            sizeInBytes += -previousItem.getSizeInBytes() + dead.getSizeInBytes();
        } else {
            sizeInBytes += dead.getSizeInBytes();
        }
    }

    private boolean isFlushNeeded(Item item) {
        return (sizeInBytes + item.getSizeInBytes()) > FLUSH_THRESHOLD_BYTES;
    }

    private void flush() {
        ByteBuffer offsets = ByteBuffer.allocate((data.size() + 1) * Long.BYTES);
        long offset = 0;
        offsets.putLong(offset);
        String fileName = System.currentTimeMillis() + ".tmp";
        String fileNameComplete = fileName.substring(0, fileName.length() - 3) + "dat";
        Path path = ssTablesDir.toPath().resolve(Paths.get(fileName));
        Path pathComplete = ssTablesDir.toPath().resolve(Paths.get(fileNameComplete));
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            for (Item v :
                    data.values()) {
                ByteBuffer key = v.getKey();
                ByteBuffer value = v.getValue();
                ByteBuffer row = ByteBuffer.allocate((int) v.getSizeInBytes());
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
