package ru.mail.polis.rubtsov;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple nano time to avoid collisions.
 */

public class TimeUtils {
    private static long millis;
    private static AtomicInteger additionalTime = new AtomicInteger();

    private TimeUtils() {
    }

    /**
     * Returns current time.
     * @return current time in nanos
     */
    
    public static long getCurrentTime() {
        final long systemCurrentTime = System.currentTimeMillis();
        if (millis != systemCurrentTime) {
            millis = systemCurrentTime;
            additionalTime.set(0);
        }
        return millis * 1_000_000 + additionalTime.getAndIncrement();
    }
}
