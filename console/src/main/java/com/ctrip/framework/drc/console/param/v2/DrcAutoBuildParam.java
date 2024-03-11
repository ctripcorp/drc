package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.dto.v2.MachineDto;

import java.util.List;
import java.util.Set;

public class DrcAutoBuildParam {
    private String srcMhaName;
    private String dstMhaName;
    private String srcDcName;
    private String dstDcName;
    private List<MachineDto> srcMachines;
    private List<MachineDto> dstMachines;

    private String buName;
    private String tag;
    private Set<String> dbName;
    private String tableFilter;

    private RowsFilterCreateParam rowsFilterCreateParam;
    private ColumnsFilterCreateParam columnsFilterCreateParam;

    private String gtidInit;

    private ViewOnlyInfo viewOnlyInfo;

    private boolean flushExistingData = true;

    public boolean isFlushExistingData() {
        return flushExistingData;
    }

    public void setFlushExistingData(boolean flushExistingData) {
        this.flushExistingData = flushExistingData;
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }

    public List<MachineDto> getSrcMachines() {
        return srcMachines;
    }

    public void setSrcMachines(List<MachineDto> srcMachines) {
        this.srcMachines = srcMachines;
    }

    public List<MachineDto> getDstMachines() {
        return dstMachines;
    }

    public void setDstMachines(List<MachineDto> dstMachines) {
        this.dstMachines = dstMachines;
    }

    public ViewOnlyInfo getViewOnlyInfo() {
        return viewOnlyInfo;
    }

    public void setViewOnlyInfo(ViewOnlyInfo viewOnlyInfo) {
        this.viewOnlyInfo = viewOnlyInfo;
    }

    public ColumnsFilterCreateParam getColumnsFilterCreateParam() {
        return columnsFilterCreateParam;
    }

    public void setColumnsFilterCreateParam(ColumnsFilterCreateParam columnsFilterCreateParam) {
        this.columnsFilterCreateParam = columnsFilterCreateParam;
    }

    public RowsFilterCreateParam getRowsFilterCreateParam() {
        return rowsFilterCreateParam;
    }

    public void setRowsFilterCreateParam(RowsFilterCreateParam rowsFilterCreateParam) {
        this.rowsFilterCreateParam = rowsFilterCreateParam;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
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

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public Set<String> getDbName() {
        return dbName;
    }

    public String getDbNameFilter() {
        if (dbName == null || dbName.isEmpty()) {
            return "";
        }
        return "(" + String.join("|", dbName) + ")";
    }

    public void setDbName(Set<String> dbName) {
        this.dbName = dbName;
    }

    public String getTableFilter() {
        return tableFilter;
    }

    public void setTableFilter(String tableFilter) {
        this.tableFilter = tableFilter;
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

    @Override
    public String toString() {
        return "DrcAutoBuildParam{" +
                "srcMhaName='" + srcMhaName + '\'' +
                ", dstMhaName='" + dstMhaName + '\'' +
                ", srcDcName='" + srcDcName + '\'' +
                ", dstDcName='" + dstDcName + '\'' +
                ", srcMachines=" + srcMachines +
                ", dstMachines=" + dstMachines +
                ", buName='" + buName + '\'' +
                ", tag='" + tag + '\'' +
                ", dbName=" + dbName +
                ", tableFilter='" + tableFilter + '\'' +
                ", rowsFilterCreateParam=" + rowsFilterCreateParam +
                ", columnsFilterCreateParam=" + columnsFilterCreateParam +
                ", viewOnlyInfo=" + viewOnlyInfo +
                '}';
    }

    public static class ViewOnlyInfo {
        /**
         * @see com.ctrip.framework.drc.console.enums.DrcStatusEnum
         */
        private Integer drcStatus;

        /**
         * @see com.ctrip.framework.drc.console.enums.DrcApplyModeEnum
         */
        private Integer drcApplyMode;
        public Integer getDrcStatus() {
            return drcStatus;
        }

        public void setDrcStatus(Integer drcStatus) {
            this.drcStatus = drcStatus;
        }

        public Integer getDrcApplyMode() {
            return drcApplyMode;
        }

        public void setDrcApplyMode(Integer drcApplyMode) {
            this.drcApplyMode = drcApplyMode;
        }
    }
}
