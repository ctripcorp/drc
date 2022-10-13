package com.ctrip.framework.drc.core.monitor.column;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/10/12
 */
public class DelayInfo {

    private String d;

    private String r;

    private String m;

    public DelayInfo() {
    }

    public DelayInfo(String dc, String region, String mha) {
        this.d = dc;
        this.r = region;
        this.m = mha;
    }

    public String getD() {
        return d;
    }

    public void setD(String dc) {
        this.d = dc;
    }

    public String getR() {
        return r;
    }

    public void setR(String region) {
        this.r = region;
    }

    public String getM() {
        return m;
    }

    public void setM(String m) {
        this.m = m;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DelayInfo)) return false;
        DelayInfo idc = (DelayInfo) o;
        return Objects.equals(d, idc.d) &&
                Objects.equals(r, idc.r) &&
                Objects.equals(m, idc.m);
    }

    @Override
    public int hashCode() {

        return Objects.hash(d, r, m);
    }

    @Override
    public String toString() {
        return "Idc{" +
                "d='" + d + '\'' +
                ", r='" + r + '\'' +
                ", m='" + m + '\'' +
                '}';
    }
}
