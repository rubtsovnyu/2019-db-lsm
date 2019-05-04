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

public class SSTable {

    private ByteBuffer records;
    private LongBuffer offsets;
    private long recordsAmount;

    public SSTable(File tableFile) {
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(
                tableFile.toPath(), StandardOpenOption.READ)) {
            ByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, tableFile.length()).order(ByteOrder.BIG_ENDIAN);
            recordsAmount = mappedByteBuffer.getLong(mappedByteBuffer.limit() - Long.BYTES);
            offsets = mappedByteBuffer.duplicate()
                    .position((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 1)))
                    .limit(mappedByteBuffer.limit() - Long.BYTES).slice().asLongBuffer();
            records = mappedByteBuffer.duplicate()
                    .limit((int) (mappedByteBuffer.limit() - Long.BYTES * (recordsAmount + 1)))
                    .slice().asReadOnlyBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer getRecord(long index) {
        long offset = offsets.get((int) index);
        long recordLimit;
        if (index == recordsAmount - 1) {
            recordLimit = records.limit();
        } else {
            recordLimit = offsets.get((int) index + 1);
        }
        return records.duplicate().position((int) offset).limit((int) recordLimit).slice().asReadOnlyBuffer();
    }

    private ByteBuffer getKey(ByteBuffer record) {
        ByteBuffer rec = record.duplicate();
        int keySize = rec.getInt();
        rec.limit(Integer.BYTES + keySize).slice().asReadOnlyBuffer();
        return rec;
    }

    private long getTimeStamp(ByteBuffer record) {
        ByteBuffer rec = record.duplicate();
        rec.position(Integer.BYTES + rec.getInt());
        return rec.getLong();
    }

    private ByteBuffer getValue(ByteBuffer record) {
        ByteBuffer rec = record.duplicate();
        int keySize = rec.getInt();
        return rec.position(Integer.BYTES + keySize + Long.BYTES * 2).slice().asReadOnlyBuffer();
    }

    private Item getItem(long pos) {
        ByteBuffer rec = getRecord(pos);
        ByteBuffer key = getKey(rec);
        long timeStamp = getTimeStamp(rec);
        ByteBuffer value;
        if (timeStamp < 0) {
            value = Item.TOMBSTONE;
        } else {
            value = getValue(rec);
        }
        return Item.of(key, value, timeStamp);
    }

    private long getPosition(ByteBuffer key) {
        long left = 0, right = recordsAmount - 1;
        while (left <= right) {
            long mid = left + (right - left) / 2;
            int compare = getKey(getRecord(mid)).compareTo(key);
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

    public Iterator<Item> iterator(ByteBuffer from) {
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
                Item item = getItem(pos);
                pos++;
                return item;
            }
        };
    }
}
