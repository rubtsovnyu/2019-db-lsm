package ru.mail.polis.rubtsov;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;

public class MyDAO implements DAO {

    private MemTable memTable;

    private ArrayList<SSTable> ssTables = new ArrayList<>();

    public MyDAO(File data, long MAX_HEAP) throws IOException {
        memTable = new MemTable(data, MAX_HEAP);
        Files.walk(data.toPath()).filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".dat")).forEach(p ->
                ssTables.add(new SSTable(p.toFile()))
        );
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        ArrayList<Iterator<Item>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (SSTable s :
                ssTables) {
            iterators.add(s.iterator(from));
        }
        Iterator<Item> collapsedIter = Iters.collapseEquals(Iterators.mergeSorted(iterators, Item.COMPARATOR), Item::getKey);
        Iterator<Item> filteredIter = Iterators.filter(collapsedIter, i -> !i.isRemoved());
        return Iterators.transform(filteredIter, i -> Record.of(i.getKey(), i.getValue()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
