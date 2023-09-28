package com.ctrip.framework.drc.applier.activity.monitor.entity;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.applier.resource.context.TransactionContextResource.RECORD_SIZE;

/**
 * Created by jixinwang on 2020/10/16
 */
public class ConflictTransactionLog {

    private String srcMhaName;

    private String destMhaName;

    private String clusterName;

    private List<String> rawSqlList;

    private List<String> rawSqlExecutedResultList;

    private List<String> destCurrentRecordList;

    private List<String> conflictHandleSqlList;

    private List<String> conflictHandleSqlExecutedResultList;

    private Long conflictHandleTime;

    private String lastResult;

    public ConflictTransactionLog() {
        this.rawSqlList = new ArrayList<>(RECORD_SIZE);
        this.rawSqlExecutedResultList = new ArrayList<>(RECORD_SIZE);
        this.destCurrentRecordList = new ArrayList<>(RECORD_SIZE);
        this.conflictHandleSqlList = new ArrayList<>(RECORD_SIZE);
        this.conflictHandleSqlExecutedResultList = new ArrayList<>(RECORD_SIZE);
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDestMhaName() {
        return destMhaName;
    }

    public void setDestMhaName(String destMhaName) {
        this.destMhaName = destMhaName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getRawSqlList() {
        return rawSqlList;
    }

    public void setRawSqlList(List<String> rawSqlList) {
        this.rawSqlList = rawSqlList;
    }

    public List<String> getRawSqlExecutedResultList() {
        return rawSqlExecutedResultList;
    }

    public void setRawSqlExecutedResultList(List<String> rawSqlExecutedResultList) {
        this.rawSqlExecutedResultList = rawSqlExecutedResultList;
    }

    public List<String> getDestCurrentRecordList() {
        return destCurrentRecordList;
    }

    public void setDestCurrentRecordList(List<String> destCurrentRecordList) {
        this.destCurrentRecordList = destCurrentRecordList;
    }

    public List<String> getConflictHandleSqlList() {
        return conflictHandleSqlList;
    }

    public void setConflictHandleSqlList(List<String> conflictHandleSqlList) {
        this.conflictHandleSqlList = conflictHandleSqlList;
    }

    public List<String> getConflictHandleSqlExecutedResultList() {
        return conflictHandleSqlExecutedResultList;
    }

    public void setConflictHandleSqlExecutedResultList(List<String> conflictHandleSqlExecutedResultList) {
        this.conflictHandleSqlExecutedResultList = conflictHandleSqlExecutedResultList;
    }

    public Long getConflictHandleTime() {
        return conflictHandleTime;
    }

    public void setConflictHandleTime(Long conflictHandleTime) {
        this.conflictHandleTime = conflictHandleTime;
    }

    public String getLastResult() {
        return lastResult;
    }

    public void setLastResult(String lastResult) {
        this.lastResult = lastResult;
    }

    @Override
    public String toString() {
        return "ConflictTransactionLog{" +
                "srcMhaName='" + srcMhaName + '\'' +
                ", destMhaName='" + destMhaName + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", rawSqlList=" + rawSqlList +
                ", rawSqlExecutedResultList=" + rawSqlExecutedResultList +
                ", destCurrentRecordList=" + destCurrentRecordList +
                ", conflictHandleSqlList=" + conflictHandleSqlList +
                ", conflictHandleSqlExecutedResultList=" + conflictHandleSqlExecutedResultList +
                ", conflictHandleTime=" + conflictHandleTime +
                ", lastResult='" + lastResult + '\'' +
                '}';
    }
}
