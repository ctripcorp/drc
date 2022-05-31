package com.ctrip.framework.drc.console.vo;

/**
 * @ClassName SimplexDrcBuildVo
 * @Author haodongPan
 * @Date 2022/5/25 10:49
 * @Version: $
 */
public class SimplexDrcBuildVo {
    private String srcMha;
    private String destMha;
    private String srcDc;
    private String destDc;
    private Long destApplierGroupId;
    private Long srcReplicatorGroupId;
    private Long srcMhaId;

    public SimplexDrcBuildVo(String srcMha, String destMha, String srcDc, String destDc, 
                             Long destApplierGroupId, Long srcReplicatorGroupId,Long srcMhaId) {
        this.srcMha = srcMha;
        this.destMha = destMha;
        this.srcDc = srcDc;
        this.destDc = destDc;
        this.srcMhaId = srcMhaId;
        this.destApplierGroupId = destApplierGroupId;
        this.srcReplicatorGroupId = srcReplicatorGroupId;
    }

    @Override
    public String toString() {
        return "SimplexDrcBuildVo{" +
                "srcMha='" + srcMha + '\'' +
                ", destMha='" + destMha + '\'' +
                ", srcDc='" + srcDc + '\'' +
                ", destDc='" + destDc + '\'' +
                ", destApplierGroupId=" + destApplierGroupId +
                ", srcReplicatorGroupId=" + srcReplicatorGroupId +
                ", srcMhaId=" + srcMhaId +
                '}';
    }

    public SimplexDrcBuildVo() {
    }

    public Long getSrcMhaId() {
        return srcMhaId;
    }

    public void setSrcMhaId(Long srcMhaId) {
        this.srcMhaId = srcMhaId;
    }

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDestMha() {
        return destMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    public String getSrcDc() {
        return srcDc;
    }

    public void setSrcDc(String srcDc) {
        this.srcDc = srcDc;
    }

    public String getDestDc() {
        return destDc;
    }

    public void setDestDc(String destDc) {
        this.destDc = destDc;
    }

    public Long getDestApplierGroupId() {
        return destApplierGroupId;
    }

    public void setDestApplierGroupId(Long destApplierGroupId) {
        this.destApplierGroupId = destApplierGroupId;
    }

    public Long getSrcReplicatorGroupId() {
        return srcReplicatorGroupId;
    }

    public void setSrcReplicatorGroupId(Long srcReplicatorGroupId) {
        this.srcReplicatorGroupId = srcReplicatorGroupId;
    }
}
