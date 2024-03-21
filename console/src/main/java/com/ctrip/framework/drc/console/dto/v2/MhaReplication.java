package com.ctrip.framework.drc.console.dto.v2;

import java.util.Objects;

/**
 * Created by dengquanliang
 * 2024/2/26 16:21
 */
public class MhaReplication {
    private String srcMhaName;
    private String dstMhaName;


    public MhaReplication() {
    }

    public MhaReplication(String srcMhaName, String dstMhaName) {
        this.srcMhaName = srcMhaName;
        this.dstMhaName = dstMhaName;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MhaReplication that = (MhaReplication) o;
        return Objects.equals(srcMhaName, that.srcMhaName) && Objects.equals(dstMhaName, that.dstMhaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcMhaName, dstMhaName);
    }
}
