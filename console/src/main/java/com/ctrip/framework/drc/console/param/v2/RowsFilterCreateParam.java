package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
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

    public RowsFilterTblV2 extractRowsFilterTbl() {
        RowsFilterTblV2 rowsFilterTbl = new RowsFilterTblV2();
        rowsFilterTbl.setMode(this.mode);
        RowsFilterConfig.Configs configs = new RowsFilterConfig.Configs();
        if (RowsFilterModeEnum.TRIP_UDL.getCode() == this.mode) {
            List<RowsFilterConfig.Parameters> parametersList = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(udlColumns)) {
                // generate udl parameters
                configs.setDrcStrategyId(this.drcStrategyId);
                RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
                parameters.setColumns(this.udlColumns);
                parameters.setUserFilterMode(UserFilterMode.Udl.getName());
                parameters.setContext(this.context);
                parameters.setIllegalArgument(this.illegalArgument);
                parameters.setFetchMode(this.fetchMode);
                parametersList.add(parameters);
            }
            if (!CollectionUtils.isEmpty(columns)) {
                // generate uid parameters
                RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
                parameters.setColumns(this.columns);
                parameters.setUserFilterMode(UserFilterMode.Uid.getName());
                parameters.setContext(this.context);
                parameters.setIllegalArgument(this.illegalArgument);
                parameters.setFetchMode(this.fetchMode);
                parametersList.add(parameters);
            }

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
