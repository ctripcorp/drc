package com.ctrip.framework.drc.console.param.v2;

import org.apache.commons.lang3.StringUtils;

public class DrcAutoBuildReq {
    private String dbName;
    private String dalClusterName;

    /**
     * @see BuildMode
     */
    private Integer mode;
    private String srcRegionName;
    private String dstRegionName;
    private String buName;
    private String tag;
    private String gtidInit;
    private TblsFilterDetail tblsFilterDetail;
    private Boolean openRowsFilterConfig;
    private RowsFilterCreateParam rowsFilterDetail;
    private Boolean openColsFilterConfig;
    private ColumnsFilterCreateParam colsFilterDetail;
    private Long applicationFormId;

    public void validAndTrim() {
        BuildMode modeEnum = getModeEnum();
        if (StringUtils.isBlank(srcRegionName)) {
            throw new IllegalArgumentException("srcRegionName should not be blank!");
        }
        if (StringUtils.isBlank(dstRegionName)) {
            throw new IllegalArgumentException("dstRegionName should not be blank!");
        }
        if (StringUtils.isBlank(buName)) {
            throw new IllegalArgumentException("buName should not be blank!");
        }
        if (StringUtils.isBlank(tag)) {
            throw new IllegalArgumentException("tag should not be blank!");
        }
        if (tblsFilterDetail == null || StringUtils.isBlank(tblsFilterDetail.tableNames)) {
            throw new IllegalArgumentException("tableName should not be blank!");
        }

        if (modeEnum == null) {
            throw new IllegalArgumentException("illegal mode: " + mode);
        }
        if (modeEnum == BuildMode.SINGLE_DB_NAME) {
            if (StringUtils.isBlank(dbName)) {
                throw new IllegalArgumentException("dbName should not be blank!");
            }
            dalClusterName = null;
            dbName = dbName.trim();
        } else if (modeEnum == BuildMode.DAL_CLUSTER_NAME) {
            if (StringUtils.isBlank(dalClusterName)) {
                throw new IllegalArgumentException("dalClusterName should not be blank!");
            }
            dbName = null;
            dalClusterName = dalClusterName.trim();
        }

        // trim
        srcRegionName = srcRegionName.trim();
        dstRegionName = dstRegionName.trim();
        buName = buName.trim();
        tag = tag.trim();
        tblsFilterDetail.tableNames = tblsFilterDetail.tableNames.trim();
    }

    public enum BuildMode {
        SINGLE_DB_NAME(0),
        DAL_CLUSTER_NAME(1),
        MULTI_DB_NAME(2)
        ;

        BuildMode(int value) {
            this.value = value;
        }

        private final int value;

        public int getValue() {
            return value;
        }
    }

    public static class TblsFilterDetail {
        public String getTableNames() {
            return tableNames;
        }

        public void setTableNames(String tableNames) {
            this.tableNames = tableNames;
        }

        private String tableNames;
    }

    public BuildMode getModeEnum() {
        if (this.mode == null) {
            return null;
        }
        for (BuildMode mode : BuildMode.values()) {
            if (mode.getValue() == this.mode) {
                return mode;
            }
        }
        return null;
    }

    public Boolean getOpenColsFilterConfig() {
        return openColsFilterConfig;
    }

    public void setOpenColsFilterConfig(Boolean openColsFilterConfig) {
        this.openColsFilterConfig = openColsFilterConfig;
    }

    public ColumnsFilterCreateParam getColsFilterDetail() {
        return colsFilterDetail;
    }

    public void setColsFilterDetail(ColumnsFilterCreateParam colsFilterDetail) {
        this.colsFilterDetail = colsFilterDetail;
    }

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

    public void setTblsFilterDetail(TblsFilterDetail tblsFilterDetail) {
        this.tblsFilterDetail = tblsFilterDetail;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDalClusterName() {
        return dalClusterName;
    }

    public void setDalClusterName(String dalClusterName) {
        this.dalClusterName = dalClusterName;
    }

    public String getSrcRegionName() {
        return srcRegionName;
    }

    public void setSrcRegionName(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }

    public String getDstRegionName() {
        return dstRegionName;
    }

    public void setDstRegionName(String dstRegionName) {
        this.dstRegionName = dstRegionName;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public TblsFilterDetail getTblsFilterDetail() {
        return tblsFilterDetail;
    }

    public RowsFilterCreateParam getRowsFilterDetail() {
        return rowsFilterDetail;
    }

    public void setRowsFilterDetail(RowsFilterCreateParam rowsFilterDetail) {
        this.rowsFilterDetail = rowsFilterDetail;
    }

    public Boolean getOpenRowsFilterConfig() {
        return openRowsFilterConfig;
    }

    public void setOpenRowsFilterConfig(Boolean openRowsFilterConfig) {
        this.openRowsFilterConfig = openRowsFilterConfig;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Long getApplicationFormId() {
        return applicationFormId;
    }

    public void setApplicationFormId(Long applicationFormId) {
        this.applicationFormId = applicationFormId;
    }

    @Override
    public String toString() {
        return "DrcAutoBuildReq{" +
                "dbName='" + dbName + '\'' +
                ", dalClusterName='" + dalClusterName + '\'' +
                ", mode=" + mode +
                ", srcRegionName='" + srcRegionName + '\'' +
                ", dstRegionName='" + dstRegionName + '\'' +
                ", buName='" + buName + '\'' +
                ", tag='" + tag + '\'' +
                ", gtidInit='" + gtidInit + '\'' +
                ", tblsFilterDetail=" + tblsFilterDetail +
                ", openRowsFilterConfig=" + openRowsFilterConfig +
                ", rowsFilterDetail=" + rowsFilterDetail +
                ", openColsFilterConfig=" + openColsFilterConfig +
                ", colsFilterDetail=" + colsFilterDetail +
                ", applicationFormId=" + applicationFormId +
                '}';
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }

}
