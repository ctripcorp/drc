package com.ctrip.framework.drc.console.monitor.consistency.table;

/**
 * Created by mingdongli
 * 2019/11/15 上午10:53.
 */
public class DefaultTableNameDescriptor implements TableNameDescriptor {

    private TableProvider tableProvider;

    private String key;

    private String onUpdate;

    public DefaultTableNameDescriptor(String table, String key, String onUpdate) {
        this.tableProvider = new DefaultTableProvider(table);
        this.key = key;
        this.onUpdate = onUpdate;
    }

    @Override
    public String getTable() {
        return tableProvider.next();
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getOnUpdate() {
        return onUpdate;
    }
}
