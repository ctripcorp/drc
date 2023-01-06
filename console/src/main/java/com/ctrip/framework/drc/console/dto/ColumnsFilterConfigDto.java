package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName ColumnsFilterConfigDto
 * @Author haodongPan
 * @Date 2023/1/3 15:47
 * @Version: $
 */
public class ColumnsFilterConfigDto {
    
    private long id;
    private long dataMediaId;
    private String mode;
    private List<String> columns;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
    
    public ColumnsFilterTbl transferTo() {
        ColumnsFilterTbl columnsFilterTbl = new ColumnsFilterTbl();
        if (id != 0) {
            columnsFilterTbl.setId(id);
        }
        if (dataMediaId == 0) {
            throw new IllegalArgumentException("dataMediaId shouldn't be zero");
        } else {
            columnsFilterTbl.setDataMediaId(dataMediaId);
        }
        if (StringUtils.isBlank(mode)) {
            throw new IllegalArgumentException("mode shouldn't be empty");
        } else {
            columnsFilterTbl.setMode(mode);
        }
        if (CollectionUtils.isEmpty(columns)) {
            throw new IllegalArgumentException("columns shouldn't be empty");
        } else {
            columnsFilterTbl.setColumns(JsonUtils.toJson(columns));
        }
        return columnsFilterTbl;
    }

    public long getDataMediaId() {
        return dataMediaId;
    }

    public void setDataMediaId(long dataMediaId) {
        this.dataMediaId = dataMediaId;
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
}
