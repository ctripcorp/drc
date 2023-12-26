package com.ctrip.framework.drc.console.vo.log;

import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/26 16:13
 */
public class ConflictRowsLogCountView {
    private List<ConflictRowsLogCount> dbCounts;
    private List<ConflictRowsLogCount> rollBackDbCounts;
    private Integer totalCount;
    private Integer rollBackTotalCount;


    public List<ConflictRowsLogCount> getDbCounts() {
        return dbCounts;
    }

    public void setDbCounts(List<ConflictRowsLogCount> dbCounts) {
        this.dbCounts = dbCounts;
    }

    public List<ConflictRowsLogCount> getRollBackDbCounts() {
        return rollBackDbCounts;
    }

    public void setRollBackDbCounts(List<ConflictRowsLogCount> rollBackDbCounts) {
        this.rollBackDbCounts = rollBackDbCounts;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Integer getRollBackTotalCount() {
        return rollBackTotalCount;
    }

    public void setRollBackTotalCount(Integer rollBackTotalCount) {
        this.rollBackTotalCount = rollBackTotalCount;
    }
}
