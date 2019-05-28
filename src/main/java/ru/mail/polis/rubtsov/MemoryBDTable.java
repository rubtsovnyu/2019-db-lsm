package ru.mail.polis.rubtsov;

import com.google.common.base.Preconditions;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static ru.mail.polis.rubtsov.BDTable.HUGE_VALUE_FILE_EXTENSION;
import static ru.mail.polis.rubtsov.BDTable.HUGE_VALUE_INDEX_FILE_EXTENSION;

public class MemoryBDTable {

    private final Path tableDataFile;
    private final Path tableIndexFile;
    private FileChannel tableIndexFC;
    private SortedMap<ByteBuffer, Rec> data;
    private List<Rec> offs;
    private long recordsAmount;

    public MemoryBDTable(final Path dataDir) throws IOException {
        final String fileName = String.valueOf(TimeUtils.getCurrentTime());
        this.tableDataFile = Files.createFile(dataDir.resolve(fileName + HUGE_VALUE_FILE_EXTENSION));
        Preconditions.checkArgument(Files.isReadable(tableDataFile));
        Preconditions.checkArgument(Files.isWritable(tableDataFile));
        this.tableIndexFile = Files.createFile(dataDir.resolve(fileName + HUGE_VALUE_INDEX_FILE_EXTENSION));
        Preconditions.checkArgument(Files.isReadable(tableIndexFile));
        Preconditions.checkArgument(Files.isWritable(tableIndexFile));
        tableIndexFC = FileChannel.open(tableIndexFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        data = new TreeMap<>();
        offs = new ArrayList<>();
        recordsAmount = 0;
    }

    void upsert(final ByteBuffer key, final InputStream value) throws IOException {
        Rec rec = Rec.of(key, TimeUtils.getCurrentTime(), tableDataFile.toFile().length());
        data.put(key, rec);
        offs.add(rec);
        recordsAmount++;
        writeValue(value);
    }

    private void writeValue(InputStream value) throws IOException {
        FileOutputStream valuesOutStream = new FileOutputStream(tableDataFile.toFile(), true);
        value.transferTo(valuesOutStream);
    }

    InputStream get(final ByteBuffer key) throws IOException {
        long offset = data.get(key).getOffset();
        FileInputStream value = new FileInputStream(tableDataFile.toFile());
        value.skip(offset);
        int indexOfRec = offs.indexOf(data.get(key));
        if (indexOfRec == offs.size() - 1) {
            value.mark((int) tableDataFile.toFile().length());
        } else {
            value.mark((int) offs.get(indexOfRec + 1).getOffset());
        }
        return value;
    }

    void writeOffset(final Rec rec) throws IOException {
        ByteBuffer row = ByteBuffer.allocate(Integer.BYTES + rec.getKey().remaining() + Long.BYTES * 2);
        row.putInt(rec.getKey().remaining())
                .put(rec.getKey().duplicate())
                .putLong(rec.getTimeStamp())
                .putLong(rec.getOffset());
        tableIndexFC.write(row);
    }

    void flush() throws IOException {
        ByteBuffer offsets = ByteBuffer.allocate((int) (recordsAmount + 1) * Long.BYTES);
        for (Rec rec : data.tailMap(Item.TOMBSTONE).values()) {
            writeOffset(rec);
            offsets.putLong(rec.getSize());
        }
        offsets.putLong(recordsAmount);
        tableIndexFC.write(offsets);
    }

    void close() throws IOException {
        if (!data.isEmpty()) {
            flush();
        } else {
            tableIndexFC.close();
            Files.delete(tableDataFile);
            Files.delete(tableIndexFile);
        }
    }
}
