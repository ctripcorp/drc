package com.ctrip.framework.drc.console.vo.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/11 10:33
 */
public class RowsFilterConfigView {
    private int mode;

    private List<String> columns;

    private List<String> udlColumns;

    private Integer drcStrategyId;

    private Integer routeStrategyId;

    private String context;

    private boolean illegalArgument;

    private Integer fetchMode;

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

    public static RowsFilterConfigView from(RowsFilterTblV2 rowsFilterTblV2) {
        RowsFilterConfigView rowsFilterConfigView = new RowsFilterConfigView();
        rowsFilterConfigView.setMode(rowsFilterTblV2.getMode());

        RowsFilterConfig.Configs configs = JsonUtils.fromJson(rowsFilterTblV2.getConfigs(), RowsFilterConfig.Configs.class);
        rowsFilterConfigView.setDrcStrategyId(configs.getDrcStrategyId());
        rowsFilterConfigView.setRouteStrategyId(configs.getRouteStrategyId());

        List<RowsFilterConfig.Parameters> parametersList = configs.getParameterList();
        RowsFilterConfig.Parameters firstParameters = parametersList.get(0);
        if (rowsFilterTblV2.getMode().equals(RowsFilterModeEnum.TRIP_UDL.getCode()) || rowsFilterTblV2.getMode().equals(RowsFilterModeEnum.TRIP_UDL_UID.getCode())) {
            setColumnsView(rowsFilterConfigView, firstParameters);
            if (parametersList.size() > 1) {
                RowsFilterConfig.Parameters secondParameters = parametersList.get(1);
                setColumnsView(rowsFilterConfigView, secondParameters);
            }
        } else {
            rowsFilterConfigView.setColumns(firstParameters.getColumns());
        }

        rowsFilterConfigView.setContext(firstParameters.getContext());
        rowsFilterConfigView.setIllegalArgument(firstParameters.getIllegalArgument());
        rowsFilterConfigView.setFetchMode(firstParameters.getFetchMode());

        return rowsFilterConfigView;
    }

    private static void setColumnsView(RowsFilterConfigView rowsFilterConfigView, RowsFilterConfig.Parameters secondParameters) {
        if (secondParameters.getUserFilterMode().equals(UserFilterMode.Udl.getName())) {
            rowsFilterConfigView.setUdlColumns(secondParameters.getColumns());
        } else if (secondParameters.getUserFilterMode().equals(UserFilterMode.Uid.getName())) {
            rowsFilterConfigView.setColumns(secondParameters.getColumns());
        }
    }
}
