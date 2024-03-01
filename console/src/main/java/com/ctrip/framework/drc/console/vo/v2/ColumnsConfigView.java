package com.ctrip.framework.drc.console.vo.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/10 19:57
 */
public class ColumnsConfigView {
    private int mode;
    private List<String> columns;

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

    public static ColumnsConfigView from(ColumnsFilterTblV2 columnsFilterTblV2) {
        ColumnsConfigView columnsConfigView = new ColumnsConfigView();
        columnsConfigView.setColumns(JsonUtils.fromJsonToList(columnsFilterTblV2.getColumns(), String.class));
        columnsConfigView.setMode(columnsFilterTblV2.getMode());
        return columnsConfigView;
    }
}
