package com.ctrip.framework.drc.console.param.log;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/11/3 3:16 下午
 */
public class ConflictApprovalCreateParam {
    //0-dstMha 1-srcMha
    private Integer writeSide;

    private List<ConflictHandleSqlDto> handleSqlDtos;

    public Integer getWriteSide() {
        return writeSide;
    }

    public void setWriteSide(Integer writeSide) {
        this.writeSide = writeSide;
    }

    public List<ConflictHandleSqlDto> getHandleSqlDtos() {
        return handleSqlDtos;
    }

    public void setHandleSqlDtos(List<ConflictHandleSqlDto> handleSqlDtos) {
        this.handleSqlDtos = handleSqlDtos;
    }
}
