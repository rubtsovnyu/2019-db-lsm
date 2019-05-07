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
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Simple LSM based {@link DAO} implementation.
 */

public class MyDAO implements DAO {
    private static final int COMPACTION_THRESHOLD = 16;

    private final MemTable memTable;
    private final List<SSTable> ssTables = new ArrayList<>();
    private final File ssTablesDir;
    private final Logger logger = Logger.getLogger("MyDAO");


    /**
     * Constructs a new, empty storage.
     *
     * @param dataFolder the folder which SSTables will be contained.
     * @param heapSizeInBytes JVM max heap size
     */

    public MyDAO(final File dataFolder, final long heapSizeInBytes) {
        memTable = new MemTable(dataFolder, heapSizeInBytes);
        ssTablesDir = dataFolder;
        try (Stream<Path> files = Files.walk(ssTablesDir.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".dat"))
                    .forEach(p -> initNewSSTable(p.toFile()));
        } catch (IOException e) {
            logger.warning("Something wrong with data.");
        }
    }

    private void initNewSSTable(final File ssTableFile) {
        try {
            final SSTable ssTable = new SSTable(ssTableFile);
            ssTable.testTable();
            ssTables.add(ssTable);
        } catch (IOException | IllegalArgumentException | IndexOutOfBoundsException e) {
            logger.warning("File corrupted: \"" + ssTableFile.getName() + "\", skipped.");
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Item> itemIterator = itemIterator(from);
        return Iterators.transform(itemIterator, i -> Record.of(i.getKey(), i.getValue()));
    }

    private Iterator<Item> itemIterator(@NotNull final ByteBuffer from) {
        final ArrayList<Iterator<Item>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable s :
                ssTables) {
            iterators.add(s.iterator(from));
        }
        final Iterator<Item> mergedIter = Iterators.mergeSorted(iterators, Item.COMPARATOR);
        final Iterator<Item> collapsedIter = Iters.collapseEquals(mergedIter, Item::getKey);
        return Iterators.filter(collapsedIter, i -> !i.isRemoved());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (memTable.isFlushNeeded(key, value)) {
            if (dropTable()) {
                memTable.upsert(key, value);
            }
        } else {
            memTable.upsert(key, value);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memTable.isFlushNeeded(key, Item.TOMBSTONE)) {
            if (dropTable()) {
                memTable.remove(key);
            }
        } else {
            memTable.remove(key);
        }
    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }

    private boolean dropTable() {
        final Path flushedFilePath = memTable.flush();
        if (flushedFilePath == null) {
            return false;
        } else {
            initNewSSTable(flushedFilePath.toFile());
            if (ssTables.size() > COMPACTION_THRESHOLD) {
                compaction();
            }
            return true;
        }
    }

    private void compaction() {
        final Iterator<Item> itemIterator = itemIterator(ByteBuffer.allocate(0));
        try (Stream<Path> files = Files.walk(ssTablesDir.toPath())) {
            final Path mergedTable = SSTable.writeNewTable(itemIterator, ssTablesDir);
            if (mergedTable != null) {
                files.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".dat"))
                        .filter(p -> !p.getFileName().toString()
                                .equals(mergedTable.getFileName().toString()))
                        .forEach(this::removeFile);
                ssTables.clear();
                initNewSSTable(mergedTable.toFile());
            }
        } catch (IOException e) {
            logger.warning("Compaction failed.");
        }
    }

    private void removeFile(final Path p) {
        try {
            Files.delete(p);
        } catch (IOException e) {
            logger.warning("Can't remove old file: " + p.getFileName().toString());
        }
    }


}
