package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class DbMqCreateDto {

    protected List<String> dbNames;
    protected String srcRegionName;
    protected LogicTableConfig logicTableConfig;
    protected MqConfigDto mqConfig;

    public void validAndTrim() {
        if (logicTableConfig == null || StringUtils.isBlank(logicTableConfig.getLogicTable())) {
            throw new IllegalArgumentException("table should not be blank!");
        }
        if (StringUtils.isBlank(srcRegionName)) {
            throw new IllegalArgumentException("srcRegionName should not be blank!");
        }
        if (CollectionUtils.isEmpty(dbNames)) {
            throw new IllegalArgumentException("dbName should not be blank!");
        }

        if (mqConfig == null) {
            throw new IllegalArgumentException("mqConfig is null!");
        }
        if (StringUtils.isBlank(mqConfig.getBu())) {
            throw new IllegalArgumentException("mqConfig.getBu should not be blank!");
        }
        if (!StringUtils.equals("qmq", mqConfig.getMqType())) {
            throw new IllegalArgumentException("mqConfig.getMqType() should be qmq!");
        }
        if (!StringUtils.equals("json", mqConfig.getSerialization())) {
            throw new IllegalArgumentException("mqConfig.getSerialization should be json!");
        }
        if (mqConfig.isOrder() && mqConfig.getOrderKey() != null) {
            mqConfig.setOrderKey(mqConfig.getOrderKey().trim());
        }

        srcRegionName = srcRegionName.trim();
        logicTableConfig.setLogicTable(logicTableConfig.getLogicTable().trim());
        logicTableConfig.setDstLogicTable(logicTableConfig.getDstLogicTable().trim());
    }

    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public String getSrcRegionName() {
        return srcRegionName;
    }

    public void setSrcRegionName(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }

    public LogicTableConfig getLogicTableConfig() {
        return logicTableConfig;
    }

    public void setLogicTableConfig(LogicTableConfig logicTableConfig) {
        this.logicTableConfig = logicTableConfig;
    }

    public MqConfigDto getMqConfig() {
        return mqConfig;
    }

    public void setMqConfig(MqConfigDto mqConfig) {
        this.mqConfig = mqConfig;
    }
}
