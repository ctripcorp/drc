package com.ctrip.framework.drc.console.param.v2.application;

/**
 * Created by dengquanliang
 * 2024/1/31 16:47
 */
public class ApplicationFormBuildParam {
    private String buName;
    private String dbName;
    private String tableName;
    private String srcRegion;
    private String dstRegion;
    private String tps;
    private String description;
    private String disruptionImpact;
    private String filterType;
    private String tag;
    private Integer flushExistingData;
    private Integer orderRelated;
    private String gtidInit;
    private String remark;
    private String applicant;

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public String getTps() {
        return tps;
    }

    public void setTps(String tps) {
        this.tps = tps;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDisruptionImpact() {
        return disruptionImpact;
    }

    public void setDisruptionImpact(String disruptionImpact) {
        this.disruptionImpact = disruptionImpact;
    }

    public String getFilterType() {
        return filterType;
    }

    public void setFilterType(String filterType) {
        this.filterType = filterType;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Integer getFlushExistingData() {
        return flushExistingData;
    }

    public void setFlushExistingData(Integer flushExistingData) {
        this.flushExistingData = flushExistingData;
    }

    public Integer getOrderRelated() {
        return orderRelated;
    }

    public void setOrderRelated(Integer orderRelated) {
        this.orderRelated = orderRelated;
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @Override
    public String toString() {
        return "ApplicationFormBuildParam{" +
                "buName='" + buName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", srcRegion='" + srcRegion + '\'' +
                ", dstRegion='" + dstRegion + '\'' +
                ", tps='" + tps + '\'' +
                ", description='" + description + '\'' +
                ", disruptionImpact='" + disruptionImpact + '\'' +
                ", filterType='" + filterType + '\'' +
                ", tag='" + tag + '\'' +
                ", flushExistingData=" + flushExistingData +
                ", orderRelated=" + orderRelated +
                ", gtidInit='" + gtidInit + '\'' +
                ", remark='" + remark + '\'' +
                ", applicant='" + applicant + '\'' +
                '}';
    }
}
