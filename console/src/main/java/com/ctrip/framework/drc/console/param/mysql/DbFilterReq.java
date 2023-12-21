package com.ctrip.framework.drc.console.param.mysql;

import java.util.Objects;

/**
 * Created by dengquanliang
 * 2023/12/20 16:32
 */
public class DbFilterReq {
    private String mha;
    private String dbFilter;

    public DbFilterReq(String mha, String dbFilter) {
        this.mha = mha;
        this.dbFilter = dbFilter;
    }

    public DbFilterReq() {
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }

    public String getDbFilter() {
        return dbFilter;
    }

    public void setDbFilter(String dbFilter) {
        this.dbFilter = dbFilter;
    }

    @Override
    public String toString() {
        return "DbFilterReq{" +
                "mha='" + mha + '\'' +
                ", dbFilter='" + dbFilter + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DbFilterReq that = (DbFilterReq) o;
        return Objects.equals(mha, that.mha) && Objects.equals(dbFilter, that.dbFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mha, dbFilter);
    }
}
