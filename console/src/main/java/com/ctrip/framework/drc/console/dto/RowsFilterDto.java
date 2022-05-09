package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName RowsFilterDto
 * @Author haodongPan
 * @Date 2022/5/6 11:15
 * @Version: $
 */
public class RowsFilterDto {
    
    private Long id;
    
    private String name;
    
    private Integer mode;
    
    private List<String> columns;
    
    private String expression;

    public RowsFilterTbl toRowsFilterTbl() throws IllegalArgumentException {

        if(!prerequisite()) {
            throw new IllegalArgumentException("some args should be not null: " + this);
        }
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setName(name);
        rowsFilterTbl.setMode(mode);
        RowsFilterConfig.Parameters parameters = new RowsFilterConfig.Parameters();
        parameters.setColumns(columns);
        parameters.setExpression(expression);
        rowsFilterTbl.setParameters(JsonUtils.toJson(parameters));
        if (id != null) {
            rowsFilterTbl.setId(id);
        }
        return rowsFilterTbl;
    }

    private boolean prerequisite() {
        return StringUtils.isNotBlank(this.name)
                && !CollectionUtils.isEmpty(columns)
                && mode != null;
    }
    
    @Override
    public String toString() {
        return "RowsFilterDto{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", mode=" + mode +
                ", columns=" + columns +
                ", expression='" + expression + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
}
