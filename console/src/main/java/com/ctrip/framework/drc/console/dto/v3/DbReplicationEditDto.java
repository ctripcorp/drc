package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.utils.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class DbReplicationEditDto extends DbReplicationCreateDto {
    private List<Long> dbReplicationIds;
    private LogicTableConfig originLogicTableConfig;

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
            throw new IllegalArgumentException("table should not be blank!");
        }
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


}
