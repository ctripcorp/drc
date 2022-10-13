package com.ctrip.framework.drc.core.monitor.column;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/10/12
 */
public class Idc {

    private String dc;

    private String region;

    public Idc() {
    }

    public Idc(String dc, String region) {
        this.dc = dc;
        this.region = region;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Idc)) return false;
        Idc idc = (Idc) o;
        return Objects.equals(dc, idc.dc) &&
                Objects.equals(region, idc.region);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dc, region);
    }

    @Override
    public String toString() {
        return "Idc{" +
                "dc='" + dc + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
