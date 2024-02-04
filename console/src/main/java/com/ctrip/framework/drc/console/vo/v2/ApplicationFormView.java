package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2024/1/31 19:21
 */
public class ApplicationFormView {
    private Long applicationFormId;
    private String buName;
    private String dbName;
    private String tableName;
    private String srcRegion;
    private String dstRegion;
    private Integer replicationType;
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
    private Integer approvalResult;
    private String operator;
    private String createTime;
    private String updateTime;

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Long getApplicationFormId() {
        return applicationFormId;
    }

    public void setApplicationFormId(Long applicationFormId) {
        this.applicationFormId = applicationFormId;
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

    public Integer getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(Integer replicationType) {
        this.replicationType = replicationType;
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

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
    }

    public Integer getApprovalResult() {
        return approvalResult;
    }

    public void setApprovalResult(Integer approvalResult) {
        this.approvalResult = approvalResult;
    }
}
