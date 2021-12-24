package com.ctrip.framework.drc.core.exception.dump;

/**
 * @Author Slight
 * Nov 05, 2019
 *
 * Exception referred to BINLOG_DUMP_GTID command.
 */
public class BinlogDumpGtidException extends Exception {
    public BinlogDumpGtidException(Throwable throwable) {
        super("Unknown", throwable);
    }

    public BinlogDumpGtidException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public BinlogDumpGtidException(String message) {
        super(message);
    }
}
