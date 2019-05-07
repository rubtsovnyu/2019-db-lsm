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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Simple LSM based {@link DAO} implementation.
 */

public class MyDAO implements DAO {
    private final MemTable memTable;

    private final List<SSTable> ssTables = new ArrayList<>();

    /**
     * Constructs a new, empty storage.
     *
     * @param dataFolder the folder which SSTables will be contained.
     * @param heapSizeInBytes JVM max heap size
     * @throws IOException if something wrong with data folder
     */
    public MyDAO(final File dataFolder, final long heapSizeInBytes) {
        memTable = new MemTable(dataFolder, heapSizeInBytes);
        try (Stream<Path> files = Files.walk(dataFolder.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".dat"))
                    .forEach(p -> initNewSSTable(p.toFile()));
        } catch (IOException e) {
            System.err.println("Something wrong with data.");
            e.printStackTrace();
        }
    }

    private void initNewSSTable(File ssTableFile) {
        try {
            ssTables.add(new SSTable(ssTableFile));
        } catch (IOException | IllegalArgumentException | IndexOutOfBoundsException e) {
            System.err.println("File corrupted: \"" + ssTableFile.getName() + "\", skipped.");
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final ArrayList<Iterator<Item>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable s :
                ssTables) {
            iterators.add(s.iterator(from));
        }
        final Iterator<Item> mergedIter = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        final Iterator<Item> collapsedIter = Iters.collapseEquals(mergedIter, Item::getKey);
        final Iterator<Item> filteredIter = Iterators.filter(collapsedIter, i -> !i.isRemoved());
        return Iterators.transform(filteredIter, i -> Record.of(i.getKey(), i.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
