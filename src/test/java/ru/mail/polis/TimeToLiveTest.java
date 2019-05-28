package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Generates random values with various time-to-live
 * and checks that they are dead after
 * expiration time (or not yet).
 *
 * @author Nikolai Rubtsov
 */
class TimeToLiveTest extends TestBase {

    private static final int ITEMS_COUNT = 1_000_000;
    private static final long LONG_LIFE = 15_000;
    private static final int VALUE_LENGTH = 16;

    @NotNull
    static ByteBuffer randomValue() {
        return TestBase.randomBuffer(VALUE_LENGTH);
    }

    /**
     * Creates values with very short life-time
     * and immediately checks that they are dead.
     */
    @Test
    void liveFastDieYoung(@TempDir File data) throws IOException, InterruptedException {
        try (DAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < ITEMS_COUNT; i++) {
                dao.upsert(TestBase.randomKey(), randomValue(), 1);
            }
            Thread.sleep(1);
            final Iterator<Record> empty = dao.iterator(ByteBuffer.allocate(0));
            assertFalse(empty.hasNext());
        }
    }

    /**
     * Creates values with 1 second life time
     * and after 1 second checks that they are dead.
     */
    @Test
    void oneSecondLife(@TempDir File data) throws IOException, InterruptedException {
        try (DAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < ITEMS_COUNT; i++) {
                dao.upsert(TestBase.randomKey(), randomValue(), 1000);
            }
            Thread.sleep(1000);
            final Iterator<Record> empty = dao.iterator(ByteBuffer.allocate(0));
            assertFalse(empty.hasNext());
        }
    }

    /**
     * Creates values with long life time, immediately checks
     * that they are not dead then waits for the expected
     * lifetime and checks that everyone is dead.
     */
    @Test
    void deadOnlyAfterTTL(@TempDir File data) throws IOException, InterruptedException {
        try (DAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < ITEMS_COUNT; i++) {
                dao.upsert(TestBase.randomKey(), randomValue(), LONG_LIFE);
            }
            final long finishTime = System.currentTimeMillis();
            final Iterator<Record> nonEmpty = dao.iterator(ByteBuffer.allocate(0));
            int counter = 0;
            while (nonEmpty.hasNext()) {
                nonEmpty.next();
                counter++;
            }
            assertEquals(counter, ITEMS_COUNT);
            final long sleepTime = System.currentTimeMillis() - finishTime;
            if (sleepTime < LONG_LIFE) {
                Thread.sleep(LONG_LIFE - sleepTime + 1);
            }
            final Iterator<Record> empty = dao.iterator(ByteBuffer.allocate(0));
            assertFalse(empty.hasNext());
        }
    }
}
