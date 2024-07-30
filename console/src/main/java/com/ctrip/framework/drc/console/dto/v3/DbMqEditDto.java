package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class DbMqEditDto extends DbMqCreateDto {
    private List<Long> dbReplicationIds;
    private LogicTableConfig originLogicTableConfig;
    private MqConfigDto originMqConfig;

    public void validAndTrim() {
        super.validAndTrim();
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            throw new IllegalArgumentException("dbReplicationIds should not be empty!");
        }
        boolean invalidId = dbReplicationIds.stream().anyMatch(e -> !NumberUtils.isPositive(e));
        if (invalidId) {
            throw new IllegalArgumentException("id should be positive!");
        }
        if (originLogicTableConfig == null || StringUtils.isBlank(originLogicTableConfig.getLogicTable())) {
            throw new IllegalArgumentException("originLogicTableConfig should not be blank!");
        }
//        if (originMqConfig == null) {
//            throw new IllegalArgumentException("originMqConfig should not be blank!");
//        }
    }

    public LogicTableConfig getOriginLogicTableConfig() {
        return originLogicTableConfig;
    }

    public void setOriginLogicTableConfig(LogicTableConfig originLogicTableConfig) {
        this.originLogicTableConfig = originLogicTableConfig;
    }


    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public MqConfigDto getOriginMqConfig() {
        return originMqConfig;
    }

    public void setOriginMqConfig(MqConfigDto originMqConfig) {
        this.originMqConfig = originMqConfig;
    }
}
