package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2025/5/20 15:53
 */
public class KafkaMessengerMigrateParam {
    private List<String> mhaNames;
    private int num;

    public KafkaMessengerMigrateParam() {
    }

    public KafkaMessengerMigrateParam(List<String> mhaNames, int num) {
        this.mhaNames = mhaNames;
        this.num = num;
    }

    public List<String> getMhaNames() {
        return mhaNames;
    }

    public void setMhaNames(List<String> mhaNames) {
        this.mhaNames = mhaNames;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
