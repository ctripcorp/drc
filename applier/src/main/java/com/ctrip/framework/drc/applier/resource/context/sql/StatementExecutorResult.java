package com.ctrip.framework.drc.applier.resource.context.sql;

/**
 * @Author Slight
 * Mar 16, 2020
 */
public class StatementExecutorResult {

    public enum TYPE {
        //batch result
        BATCHED("BATCHED"),
        //one row result
        UPDATE_COUNT_EQUALS_ZERO("UPDATE_COUNT_EQUALS_ZERO"),
        UPDATE_COUNT_EQUALS_ONE("UPDATE_COUNT_EQUALS_ONE"),
        DUPLICATE_ENTRY("DUPLICATE_ENTRY"),
        NO_PARAMETER("NO_PARAMETER"),
        UNKNOWN_COLUMN("UNKNOWN_COLUMN"),
        //error
        ERROR("ERROR"),
        UNSET("UNSET");

        String name;

        TYPE(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public final TYPE type;

    public StatementExecutorResult(TYPE type) {
        this.type = type;
    }
    public static StatementExecutorResult of(TYPE type)  {
        return new StatementExecutorResult(type);
    }

    public Throwable throwable;

    public StatementExecutorResult with(String message) {
        this.throwable = new AssertionError(message);
        return this;
    }

    public StatementExecutorResult with(Throwable throwable) {
        this.throwable = throwable;
        return this;
    }
}
