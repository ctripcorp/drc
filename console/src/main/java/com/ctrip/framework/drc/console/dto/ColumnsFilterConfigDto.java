package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/30
 */
public class ColumnsFilterConfigDto {

    private Long id ;

    private Long applierGroupId;

    private Long dataMediaId;

    private String namespace;

    private String name;

    // see DataMediaTypeEnum
    private Integer type;

    private Long dataMediaSourceId;

    private Long columnsFilterId;

    private String mode;

    private List<String> columns;

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

    public DataMediaTbl extractDataMediaTbl() {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(this.dataMediaId);
        dataMediaTbl.setNamespcae(this.namespace);
        dataMediaTbl.setName(this.name);
        dataMediaTbl.setType(this.type);
        dataMediaTbl.setDataMediaSourceId(this.dataMediaSourceId);
        return dataMediaTbl;
    }

    public ColumnsFilterTbl extractRowsFilterTbl() {
        ColumnsFilterTbl columnsFilterTbl = new ColumnsFilterTbl();
        columnsFilterTbl.setId(this.columnsFilterId);
        columnsFilterTbl.setMode(this.mode);
        columnsFilterTbl.setColumns(JsonUtils.toJson(JsonUtils.toJson(columns)));
        return columnsFilterTbl;
    }
}
