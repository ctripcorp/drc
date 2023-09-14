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
    private TblsFilterDetail tblsFilterDetail;

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
    }

    public enum BuildMode {
        SINGLE_DB_NAME(0),
        DAL_CLUSTER_NAME(1),
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
        for (BuildMode mode : BuildMode.values()) {
            if (mode.getValue() == this.mode) {
                return mode;
            }
        }
        return null;
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

}
