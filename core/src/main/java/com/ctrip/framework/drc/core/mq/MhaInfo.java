package com.ctrip.framework.drc.core.mq;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

/**
 * Created by dengquanliang
 * 2025/3/21 15:09
 */
public class MhaInfo {
    private Map<String, String> tags;
    private String mhaName;
    private String dc;
    private String mqType;


    public Map<String, String> getTags() {
        if (tags == null) {
            tags = Maps.newHashMap();
            tags.put("mhaName", mhaName);
            tags.put("dc", dc);
            tags.put("mqType", mqType);
        }
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        MhaInfo mhaInfo = (MhaInfo) o;
        return Objects.equals(mhaName, mhaInfo.mhaName) && Objects.equals(dc, mhaInfo.dc) && Objects.equals(mqType, mhaInfo.mqType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mhaName, dc, mqType);
    }

    public MhaInfo(String mhaName, String dc, String mqType) {
        this.mhaName = mhaName;
        this.dc = dc;
        this.mqType = mqType;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    @Override
    public String toString() {
        return "MhaInfo{" +
                "mhaName='" + mhaName + '\'' +
                ", dc='" + dc + '\'' +
                ", mqType='" + mqType + '\'' +
                '}';
    }
}
