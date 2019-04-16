package ru.mail.polis.MyDAO;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final SortedMap<ByteBuffer, Record> data = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return data.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        data.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        data.remove(key);
    }

    @Override
    public void close() throws IOException {
        //nothing
    }

}
