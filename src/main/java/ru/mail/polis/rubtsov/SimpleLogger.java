package ru.mail.polis.rubtsov;

/**
 * Simple logger.
 */

public final class SimpleLogger {
    private SimpleLogger() {
    }

    /**
     * Logs given message
     * @param message message that should be logged
     */

    public static void log(String message) {
        System.err.println(message);
    }
}
