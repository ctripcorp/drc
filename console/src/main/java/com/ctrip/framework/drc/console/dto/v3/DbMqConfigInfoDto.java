package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;
import java.util.Objects;

public class DbMqConfigInfoDto {
    private String srcRegionName;
    private List<String> dbNames;
    private List<MqLogicTableSummaryDto> logicTableSummaryDtos;
    private List<MhaMqDto> mhaMqDtos;

    public DbMqConfigInfoDto(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }

    public String getSrcRegionName() {
        return srcRegionName;
    }

    public void setSrcRegionName(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }



    public List<MqLogicTableSummaryDto> getLogicTableSummaryDtos() {
        return logicTableSummaryDtos;
    }

    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public void setLogicTableSummaryDtos(List<MqLogicTableSummaryDto> logicTableSummaryDtos) {
        this.logicTableSummaryDtos = logicTableSummaryDtos;
    }

    public List<MhaMqDto> getMhaMqDtos() {
        return mhaMqDtos;
    }

    public void setMhaMqDtos(List<MhaMqDto> mhaMqDtos) {
        this.mhaMqDtos = mhaMqDtos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbMqConfigInfoDto)) return false;
        DbMqConfigInfoDto that = (DbMqConfigInfoDto) o;
        return Objects.equals(srcRegionName, that.srcRegionName) && Objects.equals(dbNames, that.dbNames) && Objects.equals(logicTableSummaryDtos, that.logicTableSummaryDtos) && Objects.equals(mhaMqDtos, that.mhaMqDtos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcRegionName, dbNames, logicTableSummaryDtos, mhaMqDtos);
    }
}