package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2024/6/4 20:57
 */
public class ApplierReplicationView {
    private String srcMhaName;
    private String dstMhaName;
    private String srcDcName;
    private String dstDcName;
    private Integer type;
    private String dbName;
    //applierId, dbApplierId, messengerId
    private Long relatedId;

    public Long getRelatedId() {
        return relatedId;
    }

    public void setRelatedId(Long relatedId) {
        this.relatedId = relatedId;
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

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public void setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
