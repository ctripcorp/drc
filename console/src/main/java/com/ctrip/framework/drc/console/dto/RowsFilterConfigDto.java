package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.xpipe.codec.JsonCodec;

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
    
    private Long dataMediaId;

    private String namespace;

    private String name;

    private Integer type;

    private Long dataMediaSourceId;

    private String dataMediaSourceName;


    private Long rowsFilterId;

    private String mode;

    private List<String> columns;

    private String context;
    
    private boolean illegalArgument;
    
    

    public RowsFilterMappingTbl getRowsFilterMappingTbl () {
        RowsFilterMappingTbl rowsFilterMappingTbl = new RowsFilterMappingTbl();
        rowsFilterMappingTbl.setId(this.id);
        rowsFilterMappingTbl.setApplierGroupId(this.applierGroupId);
        rowsFilterMappingTbl.setDataMediaId(this.dataMediaId);
        rowsFilterMappingTbl.setRowsFilterId(this.rowsFilterId);
        return rowsFilterMappingTbl;
    }
    
    public DataMediaTbl getDataMediaTbl() {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(this.dataMediaId);
        dataMediaTbl.setNamespcae(this.namespace);
        dataMediaTbl.setName(this.name);
        dataMediaTbl.setType(this.type);
        dataMediaTbl.setDataMediaSourceId(this.dataMediaSourceId);
        return dataMediaTbl;
    }

    public RowsFilterTbl getRowsFilterTbl() {
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(this.getRowsFilterId());
        rowsFilterTbl.setMode(this.getMode());
        RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
        parameters.setColumns(this.getColumns());
        parameters.setContext(this.getContext());
        parameters.setIllegalArgument(this.getIllegalArgument());
        rowsFilterTbl.setParameters(JsonCodec.INSTANCE.encode(parameters));
        return rowsFilterTbl;
    }


    @Override
    public String toString() {
        return "RowsFilterConfigDto{" +
                "id=" + id +
                ", applierGroupId=" + applierGroupId +
                ", dataMediaId=" + dataMediaId +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                ", rowsFilterId=" + rowsFilterId +
                ", mode='" + mode + '\'' +
                ", columns=" + columns +
                ", context='" + context + '\'' +
                ", illegalArgument=" + illegalArgument +
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

    public Long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(Long applierGroupId) {
        this.applierGroupId = applierGroupId;
    }

    public boolean getIllegalArgument() {
        return illegalArgument;
    }

    public void setIllegalArgument(boolean illegalArgument) {
        this.illegalArgument = illegalArgument;
    }
}
