package com.ctrip.framework.drc.core.driver.schema.data;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author Slight
 * Oct 11, 2019
 */
public class Schemas extends HashMap<TableKey, Columns> {

    public static Schemas empty() {
        return new Schemas();
    }

    private Schemas() {}

    public static Schemas from(Map<TableKey, Columns> tables) {
        return new Schemas(tables);
    }

    public Schemas(Map<TableKey, Columns> tables) {
        this.putAll(tables);
    }

    public Columns getColumns(String databaseName, String tableName) {
        return get(TableKey.from(databaseName, tableName));
    }

    public Schemas merge(TableKey tableKey, Columns columns) {
        put(tableKey, columns);
        return this;
    }

    public Schemas merge(Map<TableKey, Columns> another) {
        Schemas merged = clone();
        merged.putAll(another);
        return merged;
    }

    public Schemas clone() {
        Schemas cloned = new Schemas();
        cloned.putAll(this);
        return cloned;
    }
}
