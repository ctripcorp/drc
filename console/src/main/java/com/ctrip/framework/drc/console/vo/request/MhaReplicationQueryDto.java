package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.http.PageReq;

import java.io.Serializable;
import java.util.Objects;


public class MhaReplicationQueryDto extends PageReq implements Serializable {
    private MhaQueryDto srcMha;
    private MhaQueryDto dstMha;

    public MhaQueryDto getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(MhaQueryDto srcMha) {
        this.srcMha = srcMha;
    }

    public MhaQueryDto getDstMha() {
        return dstMha;
    }

    public void setDstMha(MhaQueryDto dstMha) {
        this.dstMha = dstMha;
    }

    @Override
    public String toString() {
        return "MhaReplicationQueryDto{" +
                "srcMha=" + srcMha +
                ", dstMha=" + dstMha +
                "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaReplicationQueryDto)) return false;
        if (!super.equals(o)) return false;
        MhaReplicationQueryDto that = (MhaReplicationQueryDto) o;
        return Objects.equals(srcMha, that.srcMha) && Objects.equals(dstMha, that.dstMha);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), srcMha, dstMha);
    }
}
