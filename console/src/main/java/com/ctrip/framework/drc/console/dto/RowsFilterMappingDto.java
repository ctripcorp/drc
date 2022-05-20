package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;

/**
 * @ClassName RowsFilterMappingDto
 * @Author haodongPan
 * @Date 2022/5/20 11:42
 * @Version: $
 */
public class RowsFilterMappingDto {
    
    private Long id;
    
    private Long applierGroupId;
    
    private Long dataMediaId;
    
    private Long rowsFilterId;

    
    public RowsFilterMappingTbl transferToTbl () {
        RowsFilterMappingTbl tbl = new RowsFilterMappingTbl();
        tbl.setId(id);
        tbl.setApplierGroupId(applierGroupId);
        tbl.setDataMediaId(dataMediaId);
        tbl.setRowsFilterId(rowsFilterId);
        return tbl;
    }
    
    @Override
    public String toString() {
        return "RowsFilterMappingDto{" +
                "id=" + id +
                ", applierGroupId=" + applierGroupId +
                ", dataMediaId=" + dataMediaId +
                ", rowsFilterId=" + rowsFilterId +
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

    public Long getDataMediaId() {
        return dataMediaId;
    }

    public void setDataMediaId(Long dataMediaId) {
        this.dataMediaId = dataMediaId;
    }

    public Long getRowsFilterId() {
        return rowsFilterId;
    }

    public void setRowsFilterId(Long rowsFilterId) {
        this.rowsFilterId = rowsFilterId;
    }
}
