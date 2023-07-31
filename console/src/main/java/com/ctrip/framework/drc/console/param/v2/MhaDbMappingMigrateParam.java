package com.ctrip.framework.drc.console.param.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/30 18:45
 */
public class MhaDbMappingMigrateParam {
    private String mhaName;
    private List<String > dbs;

    public MhaDbMappingMigrateParam(String mhaName, List<String> dbs) {
        this.mhaName = mhaName;
        this.dbs = dbs;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }

    @Override
    public String toString() {
        return "MhaDbMappingMigrateParam{" +
                "mhaName='" + mhaName + '\'' +
                ", dbs=" + dbs +
                '}';
    }
}
