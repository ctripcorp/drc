package com.ctrip.framework.drc.console.dto.v3;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LogicTableSummaryDto {
    private List<Long> dbReplicationIds;
    private LogicTableConfig config;
    private Timestamp datachangeLasttime;

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

    public LogicTableSummaryDto(List<Long> dbReplicationIds, LogicTableConfig config, Timestamp datachangeLasttime) {
        this.dbReplicationIds = dbReplicationIds;
        this.config = config;
        this.datachangeLasttime = datachangeLasttime;
    }

    public static List<LogicTableSummaryDto> fromDbReplication(List<DbReplicationDto> collect1){
        return collect1.stream()
                .collect(Collectors.groupingBy(DbReplicationDto::getLogicTableConfig))
                .entrySet().stream()
                .map(entry -> {
                    List<DbReplicationDto> value = entry.getValue();
                    Timestamp latestTimestamp = value.stream()
                            .map(DbReplicationDto::getDatachangeLasttime)
                            .filter(Objects::nonNull)
                            .max(Timestamp::compareTo)
                            .orElse(null);
                    return new LogicTableSummaryDto(value.stream().map(DbReplicationDto::getDbReplicationId).collect(Collectors.toList()),
                            entry.getKey(),latestTimestamp);
                })
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

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}
