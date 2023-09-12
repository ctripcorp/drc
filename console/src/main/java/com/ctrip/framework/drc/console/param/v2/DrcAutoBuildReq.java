package com.ctrip.framework.drc.console.param.v2;

public class DrcAutoBuildReq {
    private String dbName;
    private String buName;
    private String srcMhaName;
    private String dstMhaName;
    private TblsFilterDetail tblsFilterDetail;

    public static class TblsFilterDetail {
        public String getTableNames() {
            return tableNames;
        }

        public void setTableNames(String tableNames) {
            this.tableNames = tableNames;
        }

        private String tableNames;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public TblsFilterDetail getTblsFilterDetail() {
        return tblsFilterDetail;
    }

    public void setTblsFilterDetail(TblsFilterDetail tblsFilterDetail) {
        this.tblsFilterDetail = tblsFilterDetail;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }
}
