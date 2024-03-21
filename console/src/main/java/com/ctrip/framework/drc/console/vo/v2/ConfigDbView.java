package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/30 11:18
 */
public class ConfigDbView {
    private List<String> dbNames;
    private int size;

    public ConfigDbView(List<String> dbNames, int size) {
        this.dbNames = dbNames;
        this.size = size;
    }

    public ConfigDbView() {
    }

    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
