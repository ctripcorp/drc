package com.ctrip.framework.drc.console.vo.display;


import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @ClassName RowsFilterMappingVo
 * @Author haodongPan
 * @Date 2022/5/18 21:46
 * @Version: $
 */
public class RowsFilterMappingVo {
    
    private Long id ;
    
    
    private Long dataMediaId;

    private String namespace;

    private String name;

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

    public RowsFilterMappingVo() {
    }

    public RowsFilterMappingVo (RowsFilterMappingTbl mapping, DataMediaTbl dataMedia, RowsFilterTbl rowsFilterTbl) {
        this.setId(mapping.getId());
        this.setRowsFilter(rowsFilterTbl);
        this.setDataMedia(dataMedia);
    }
    

    public void setDataMedia(DataMediaTbl dataMedia) {
        this.dataMediaId = dataMedia.getId();
        this.namespace = dataMedia.getNamespcae();
        this.name = dataMedia.getName();
        this.type = dataMedia.getType();
        this.dataMediaSourceId = dataMedia.getDataMediaSourceId();
    }
    
    public void setRowsFilter(RowsFilterTbl rowsFilterTbl) {
        this.rowsFilterId = rowsFilterTbl.getId();
        this.columns = Lists.newArrayList();
        this.udlColumns = Lists.newArrayList();

        RowsFilterConfig.Configs configs;
        // for migrate
        if (RowsFilterType.TripUid.getName().equals(rowsFilterTbl.getMode())) {
            this.mode = RowsFilterType.TripUdl.getName();
            configs = new RowsFilterConfig.Configs();
            configs.setParameterList(Lists.newArrayList(
                    JsonUtils.fromJson(rowsFilterTbl.getParameters(), RowsFilterConfig.Parameters.class)
            ));
        } else {
            this.mode = rowsFilterTbl.getMode();
            configs =  JsonUtils.fromJson(rowsFilterTbl.getConfigs(), RowsFilterConfig.Configs.class);
        }

        this.drcStrategyId = configs.getDrcStrategyId();
        this.routeStrategyId = configs.getRouteStrategyId();
        
        List<RowsFilterConfig.Parameters> parametersList= configs.getParameterList();
        if (RowsFilterType.TripUdl.getName().equalsIgnoreCase(rowsFilterTbl.getMode())) {
            for (RowsFilterConfig.Parameters parameters : parametersList) {
                this.context = parameters.getContext();
                this.illegalArgument = parameters.getIllegalArgument();
                this.fetchMode = parameters.getFetchMode();
                if (UserFilterMode.Uid.getName().equals(parameters.getUserFilterMode())) {
                    this.columns = parameters.getColumns();
                } 
                if(UserFilterMode.Udl.getName().equals(parameters.getUserFilterMode())){
                    this.udlColumns = parameters.getColumns();
                }
            }
        } else {
            RowsFilterConfig.Parameters parameters = parametersList.get(0);
            this.columns = parameters.getColumns();
            this.context = parameters.getContext();
            this.illegalArgument = parameters.getIllegalArgument();
            this.fetchMode = parameters.getFetchMode();
        }
        
    }

    @Override
    public String toString() {
        return "RowsFilterMappingVo{" +
                "id=" + id +
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
}
