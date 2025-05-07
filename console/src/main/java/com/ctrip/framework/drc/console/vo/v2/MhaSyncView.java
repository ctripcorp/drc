package com.ctrip.framework.drc.console.vo.v2;

import java.util.Set;

/**
 * Created by shiruixin
 * 2024/7/12 10:52
 * mhaSyncIdSet: mha同步数
 * dbNameSet：db数
 * dbSyncSet：db链路同步数
 * dalClusterSet: dalcluster数
 * dbMessengerSet: messenger 接入DB数
 * dbOtterSet: otter 接入DB数
 */
public class MhaSyncView {
    private Set<Long> mhaSyncIdSet;
    private  Set<String> dbNameSet;
    private  Set<String> dbSyncSet;
    private Set<String> dalClusterSet;
    private Set<String> dbMessengerSet;
    private Set<String> dbOtterSet;

    public Set<Long> getMhaSyncIds() {
        return mhaSyncIdSet;
    }

    public void setMhaSyncIds(Set<Long> mhaSyncIds) {
        this.mhaSyncIdSet = mhaSyncIds;
    }

    public Set<String> getDbNameSet() {
        return dbNameSet;
    }

    public void setDbNameSet(Set<String> dbNameSet) {
        this.dbNameSet = dbNameSet;
    }

    public Set<String> getDbSyncSet() {
        return dbSyncSet;
    }

    public void setDbSyncSet(Set<String> dbSyncSet) {
        this.dbSyncSet = dbSyncSet;
    }

    public Set<String> getDalClusterSet() {
        return dalClusterSet;
    }

    public void setDalClusterSet(Set<String> dalClusterSet) {
        this.dalClusterSet = dalClusterSet;
    }

    public Set<String> getDbMessengerSet() {
        return dbMessengerSet;
    }

    public void setDbMessengerSet(Set<String> dbMessengerSet) {
        this.dbMessengerSet = dbMessengerSet;
    }

    public Set<String> getDbOtterSet() {
        return dbOtterSet;
    }

    public void setDbOtterSet(Set<String> dbOtterSet) {
        this.dbOtterSet = dbOtterSet;
    }
}
