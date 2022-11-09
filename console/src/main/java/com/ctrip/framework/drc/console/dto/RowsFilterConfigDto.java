package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName RowsFilterConfigDto
 * @Author haodongPan
 * @Date 2022/5/26 11:18
 * @Version: $
 */
public class RowsFilterConfigDto {
    private Long id ;
    
    private Long applierGroupId;
    
    // see ApplierTypeEnum,default 0
    private int applierType = 0;
    
    private Long dataMediaId;

    private String namespace;

    private String name;

    // see DataMediaTypeEnum
    private Integer type;

    private Long dataMediaSourceId;

    private String dataMediaSourceName;
    
    private Long rowsFilterId;

    private String mode;

    private List<String> columns;

    private List<String> udlColumns;
    
    private Integer drcStrategyId;
    
    private Integer routeStrategyId;

    private String context;
    
    private boolean illegalArgument;
    
    private Integer fetchMode;
    
    
    public RowsFilterMappingTbl extractRowsFilterMappingTbl() {
        RowsFilterMappingTbl rowsFilterMappingTbl = new RowsFilterMappingTbl();
        rowsFilterMappingTbl.setId(this.id);
        rowsFilterMappingTbl.setApplierGroupId(this.applierGroupId);
        rowsFilterMappingTbl.setType(this.applierType);
        rowsFilterMappingTbl.setDataMediaId(this.dataMediaId);
        rowsFilterMappingTbl.setRowsFilterId(this.rowsFilterId);
        return rowsFilterMappingTbl;
    }
    
    public DataMediaTbl extractDataMediaTbl() {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(this.dataMediaId);
        dataMediaTbl.setNamespcae(this.namespace);
        dataMediaTbl.setName(this.name);
        dataMediaTbl.setType(this.type);
        dataMediaTbl.setDataMediaSourceId(this.dataMediaSourceId);
        return dataMediaTbl;
    }

    public RowsFilterTbl extractRowsFilterTbl() {
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(this.rowsFilterId);
        rowsFilterTbl.setMode(this.mode);
        RowsFilterConfig.Configs configs = new RowsFilterConfig.Configs();
        if (RowsFilterType.TripUdl.getName().equalsIgnoreCase(this.mode)) {
            List<RowsFilterConfig.Parameters> parametersList = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(udlColumns)) {
                // generate udl parameters
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
            configs.setDrcStrategyId(this.drcStrategyId);
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
        return "RowsFilterConfigDto{" +
                "id=" + id +
                ", applierGroupId=" + applierGroupId +
                ", applierType=" + applierType +
                ", dataMediaId=" + dataMediaId +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                ", rowsFilterId=" + rowsFilterId +
                ", mode='" + mode + '\'' +
                ", columns=" + columns +
                ", udlColumns=" + udlColumns +
                ", drcStrategyId=" + drcStrategyId +
                ", routeStrategyId=" + routeStrategyId +
                ", context='" + context + '\'' +
                ", illegalArgument=" + illegalArgument +
                ", fetchMode=" + fetchMode +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(Long applierGroupId) {
        this.applierGroupId = applierGroupId;
    }

    public int getApplierType() {
        return applierType;
    }

    public void setApplierType(int applierType) {
        this.applierType = applierType;
    }

    public Long getDataMediaId() {
        return dataMediaId;
    }

    public void setDataMediaId(Long dataMediaId) {
        this.dataMediaId = dataMediaId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getDataMediaSourceId() {
        return dataMediaSourceId;
    }

    public void setDataMediaSourceId(Long dataMediaSourceId) {
        this.dataMediaSourceId = dataMediaSourceId;
    }

    public String getDataMediaSourceName() {
        return dataMediaSourceName;
    }

    public void setDataMediaSourceName(String dataMediaSourceName) {
        this.dataMediaSourceName = dataMediaSourceName;
    }

    public Long getRowsFilterId() {
        return rowsFilterId;
    }

    public void setRowsFilterId(Long rowsFilterId) {
        this.rowsFilterId = rowsFilterId;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
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
