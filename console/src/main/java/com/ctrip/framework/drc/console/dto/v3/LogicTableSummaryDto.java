package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;
import java.util.stream.Collectors;

public class LogicTableSummaryDto {
    private List<Long> dbReplicationIds;
    private LogicTableConfig config;

    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public LogicTableConfig getConfig() {
        return config;
    }

    public void setConfig(LogicTableConfig config) {
        this.config = config;
    }

    public LogicTableSummaryDto(List<Long> dbReplicationIds, LogicTableConfig config) {
        this.dbReplicationIds = dbReplicationIds;
        this.config = config;
    }

    public static List<LogicTableSummaryDto> fromDbReplication(List<DbReplicationDto> collect1){
        return collect1.stream()
                .collect(Collectors.groupingBy(DbReplicationDto::getLogicTableConfig))
                .entrySet().stream()
                .map(entry -> new LogicTableSummaryDto(entry.getValue().stream().map(DbReplicationDto::getDbReplicationId).collect(Collectors.toList()), entry.getKey()))
                .collect(Collectors.toList());
    }
    public static List<LogicTableSummaryDto> from(List<MhaDbReplicationDto> mhaDbReplicationDtos){
        List<DbReplicationDto> dbReplicationDtos = mhaDbReplicationDtos.stream().flatMap(e -> e.getDbReplicationDtos().stream()).collect(Collectors.toList());
        return fromDbReplication(dbReplicationDtos);
    }

    @Override
    public String toString() {
        return "LogicTableSummaryDto{" +
                "dbReplicationIds=" + dbReplicationIds +
                ", config=" + config +
                '}';
    }
}
