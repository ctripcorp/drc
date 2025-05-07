package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.core.mq.MqType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class DbMqCreateDto {
    protected String dalclusterName;
    protected List<String> dbNames;
    protected String srcRegionName;
    protected LogicTableConfig logicTableConfig;
    protected MqConfigDto mqConfig;
    private boolean notPermitSameTableMqConfig;

    public void validAndTrim() {
        if (StringUtils.isBlank(dalclusterName)) {
            throw new IllegalArgumentException("dalclusterName should not be blank!");
        }
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
        String mqType = mqConfig.getMqType();
        if (MqType.parse(mqType) == null) {
            throw new IllegalArgumentException("mqConfig.mqType not valid: " + mqType);
        }
        if (!StringUtils.equals("json", mqConfig.getSerialization())) {
            throw new IllegalArgumentException("mqConfig.getSerialization should be json!");
        }
        if (mqConfig.isOrder() && mqConfig.getOrderKey() != null) {
            mqConfig.setOrderKey(mqConfig.getOrderKey().trim());
        }

        if (!CollectionUtils.isEmpty(mqConfig.getFilterFields())) {
            List<String> lowerCaseString = mqConfig.getFilterFields().stream()
                    .map(String::toLowerCase).toList();
            mqConfig.setFilterFields(lowerCaseString);
        }

        srcRegionName = srcRegionName.trim();
        logicTableConfig.setLogicTable(logicTableConfig.getLogicTable().trim());
        logicTableConfig.setDstLogicTable(logicTableConfig.getDstLogicTable().trim());
        this.mqConfig.setTopic(logicTableConfig.getDstLogicTable());
    }

    public String getDalclusterName() {
        return dalclusterName;
    }

    public void setDalclusterName(String dalClusterName) {
        this.dalclusterName = dalClusterName;
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

    public boolean isNotPermitSameTableMqConfig() {
        return notPermitSameTableMqConfig;
    }

    public void setNotPermitSameTableMqConfig(boolean notPermitSameTableMqConfig) {
        this.notPermitSameTableMqConfig = notPermitSameTableMqConfig;
    }
}
