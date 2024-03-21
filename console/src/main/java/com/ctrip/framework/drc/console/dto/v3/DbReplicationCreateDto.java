package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.param.v2.ColumnsFilterCreateParam;
import com.ctrip.framework.drc.console.param.v2.RowsFilterCreateParam;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class DbReplicationCreateDto {

    protected List<String> dbNames;
    protected String srcRegionName;
    protected String dstRegionName;
    protected LogicTableConfig logicTableConfig;
    protected RowsFilterCreateParam rowsFilterCreateParam;
    protected ColumnsFilterCreateParam columnsFilterCreateParam;

    public void validAndTrim() {
        if (logicTableConfig == null || StringUtils.isBlank(logicTableConfig.getLogicTable())) {
            throw new IllegalArgumentException("table should not be blank!");
        }
        if (StringUtils.isBlank(srcRegionName)) {
            throw new IllegalArgumentException("srcRegionName should not be blank!");
        }
        if (StringUtils.isBlank(dstRegionName)) {
            throw new IllegalArgumentException("dstRegionName should not be blank!");
        }
        if (CollectionUtils.isEmpty(dbNames)) {
            throw new IllegalArgumentException("dbName should not be blank!");
        }

        srcRegionName = srcRegionName.trim();
        dstRegionName = dstRegionName.trim();
        logicTableConfig.setLogicTable(logicTableConfig.getLogicTable().trim());
    }


    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public LogicTableConfig getLogicTableConfig() {
        return logicTableConfig;
    }

    public void setLogicTableConfig(LogicTableConfig logicTableConfig) {
        this.logicTableConfig = logicTableConfig;
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
}
