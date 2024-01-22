package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/1 16:30
 */
public class RowsFilterCreateParam {
    private List<Long> dbReplicationIds;

    private int mode;

    private List<String> columns;

    private List<String> udlColumns;

    private Integer drcStrategyId;

    private Integer routeStrategyId;

    private String context;

    private boolean illegalArgument;

    private Integer fetchMode;

    public void checkParam() {
        boolean hasUdlColumns = !CollectionUtils.isEmpty(udlColumns);
        boolean hasColumns = !CollectionUtils.isEmpty(columns);
        if (this.mode == RowsFilterModeEnum.TRIP_UDL.getCode() || this.mode == RowsFilterModeEnum.TRIP_UDL_UID.getCode()) {
            if (!hasColumns && !hasUdlColumns) {
                throw new IllegalArgumentException("columns empty, not allowed!");
            }
            boolean configBothUidAndUdl = hasColumns && hasUdlColumns;
            if (this.mode == RowsFilterModeEnum.TRIP_UDL.getCode() && configBothUidAndUdl) {
                throw new IllegalArgumentException("config both uid & udl filter, not allowed!");
            }
            if (this.mode == RowsFilterModeEnum.TRIP_UDL_UID.getCode() && !configBothUidAndUdl) {
                throw new IllegalArgumentException("should config both udl and uid!");
            }
        } else {
            if (!hasColumns) {
                throw new IllegalArgumentException("should config columns!");
            }
            if (StringUtils.isEmpty(this.context)) {
                throw new IllegalArgumentException("context should not be empty!");
            }
        }
    }

    public RowsFilterTblV2 extractRowsFilterTbl() {
        this.checkParam();
        RowsFilterTblV2 rowsFilterTbl = new RowsFilterTblV2();
        rowsFilterTbl.setMode(this.mode);
        RowsFilterConfig.Configs configs = new RowsFilterConfig.Configs();
        if (RowsFilterModeEnum.TRIP_UDL.getCode() == this.mode) {
            List<RowsFilterConfig.Parameters> parametersList = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(udlColumns)) {
                // generate udl parameters
                configs.setDrcStrategyId(this.drcStrategyId);
                parametersList.add(this.getUdlParameters());
            }
            if (!CollectionUtils.isEmpty(columns)) {
                // generate uid parameters
                parametersList.add(this.getUidParameters());
            }

            configs.setParameterList(parametersList);
            configs.setRouteStrategyId(this.routeStrategyId);
        } else if (RowsFilterModeEnum.TRIP_UDL_UID.getCode() == this.mode) {
            List<RowsFilterConfig.Parameters> parametersList = Lists.newArrayList();
            parametersList.add(this.getUdlParameters());
            parametersList.add(this.getUidParameters());
            configs.setDrcStrategyId(this.drcStrategyId);

            configs.setParameterList(parametersList);
            configs.setRouteStrategyId(this.routeStrategyId);
        } else {
            // generate parameter
            RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
            parameters.setColumns(this.columns);
            parameters.setContext(this.context);
            parameters.setIllegalArgument(this.illegalArgument);
            parameters.setFetchMode(this.fetchMode);

            configs.setParameterList(Lists.newArrayList(parameters));
        }
        rowsFilterTbl.setConfigs(JsonUtils.toJson(configs));
        return rowsFilterTbl;
    }

    private RowsFilterConfig.Parameters getUidParameters() {
        RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
        parameters.setColumns(this.columns);
        parameters.setUserFilterMode(UserFilterMode.Uid.getName());
        parameters.setContext(this.context);
        parameters.setIllegalArgument(this.illegalArgument);
        parameters.setFetchMode(this.fetchMode);
        return parameters;
    }

    private RowsFilterConfig.Parameters getUdlParameters() {
        RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
        parameters.setColumns(this.udlColumns);
        parameters.setUserFilterMode(UserFilterMode.Udl.getName());
        parameters.setContext(this.context);
        parameters.setIllegalArgument(this.illegalArgument);
        parameters.setFetchMode(this.fetchMode);
        return parameters;
    }

    @Override
    public String toString() {
        return "RowsFilterCreateParam{" +
                "dbReplicationIds=" + dbReplicationIds +
                ", mode=" + mode +
                ", columns=" + columns +
                ", udlColumns=" + udlColumns +
                ", drcStrategyId=" + drcStrategyId +
                ", routeStrategyId=" + routeStrategyId +
                ", context='" + context + '\'' +
                ", illegalArgument=" + illegalArgument +
                ", fetchMode=" + fetchMode +
                '}';
    }

    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getUdlColumns() {
        return udlColumns;
    }

    public void setUdlColumns(List<String> udlColumns) {
        this.udlColumns = udlColumns;
    }

    public Integer getDrcStrategyId() {
        return drcStrategyId;
    }

    public void setDrcStrategyId(Integer drcStrategyId) {
        this.drcStrategyId = drcStrategyId;
    }

    public Integer getRouteStrategyId() {
        return routeStrategyId;
    }

    public void setRouteStrategyId(Integer routeStrategyId) {
        this.routeStrategyId = routeStrategyId;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public boolean isIllegalArgument() {
        return illegalArgument;
    }

    public void setIllegalArgument(boolean illegalArgument) {
        this.illegalArgument = illegalArgument;
    }

    public Integer getFetchMode() {
        return fetchMode;
    }

    public void setFetchMode(Integer fetchMode) {
        this.fetchMode = fetchMode;
    }
}
