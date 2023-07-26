package com.ctrip.framework.drc.applier.activity.monitor.entity;

import com.ctrip.framework.drc.applier.activity.monitor.ConflictType;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;

/**
 * @ClassName ConflictTable
 * @Author haodongPan
 * @Date 2023/6/7 15:32
 * @Version: $
 */
public class ConflictTable {
    
    private String db;
    private String table;
    private ConflictType type;   // 1-commit,0-rollback


    public Map<String,String> generateTags() {
        Map<String,String> tags = Maps.newHashMap();
        tags.put("db",db);
        tags.put("table",table);
        return tags;
    }
    
    
    public ConflictTable() {
    }

    public ConflictTable(String db, String table, ConflictType type) {
        this.db = db;
        this.table = table;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConflictTable that = (ConflictTable) o;
        return type.getVal() == that.type.getVal() && Objects.equals(db, that.db) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table, type.getVal());
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }


    public ConflictType getType() {
        return type;
    }

    public void setType(ConflictType type) {
        this.type = type;
    }
}