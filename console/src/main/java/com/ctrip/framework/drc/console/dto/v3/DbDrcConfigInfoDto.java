package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;
import java.util.Objects;

public class DbDrcConfigInfoDto {
    private String srcRegionName;
    private String dstRegionName;
    private List<String> dbNames;
    private List<LogicTableSummaryDto> logicTableSummaryDtos;
    private List<MhaReplicationDto> mhaReplications;

    public DbDrcConfigInfoDto(String srcRegionName, String dstRegionName) {
        this.srcRegionName = srcRegionName;
        this.dstRegionName = dstRegionName;
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

    public List<LogicTableSummaryDto> getLogicTableSummaryDtos() {
        return logicTableSummaryDtos;
    }

    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public void setLogicTableSummaryDtos(List<LogicTableSummaryDto> logicTableSummaryDtos) {
        this.logicTableSummaryDtos = logicTableSummaryDtos;
    }

    public List<MhaReplicationDto> getMhaReplications() {
        return mhaReplications;
    }

    public void setMhaReplications(List<MhaReplicationDto> mhaReplications) {
        this.mhaReplications = mhaReplications;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbDrcConfigInfoDto)) return false;
        DbDrcConfigInfoDto that = (DbDrcConfigInfoDto) o;
        return Objects.equals(srcRegionName, that.srcRegionName) && Objects.equals(dstRegionName, that.dstRegionName) && Objects.equals(logicTableSummaryDtos, that.logicTableSummaryDtos) && Objects.equals(mhaReplications, that.mhaReplications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcRegionName, dstRegionName, logicTableSummaryDtos, mhaReplications);
    }

    @Override
    public String toString() {
        return "DbDrcConfigInfoDto{" +
                "srcRegionName='" + srcRegionName + '\'' +
                ", dstRegionName='" + dstRegionName + '\'' +
                ", dbNames=" + dbNames +
                ", logicTableSummaryDtos=" + logicTableSummaryDtos +
                ", mhaReplications=" + mhaReplications +
                '}';
    }
}