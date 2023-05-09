package com.ctrip.framework.drc.console.vo.api;

import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * @ClassName DrcDbInfo
 * @Author haodongPan
 * @Date 2023/4/28 10:55
 * @Version: $
 */
public class DrcDbInfo {
    
    private String db;
    
    private String table;
    
    private String srcMha;

    private String destMha;
    
    private String srcRegion;
    
    private String destRegion;
    
    private List<RowsFilterConfig> rowsFilterConfigs = Lists.newArrayList();
    private List<ColumnsFilterConfig> columnsFilterConfigs = Lists.newArrayList();


    public void addRegexTable(String tableRegex) {
        if(StringUtils.isEmpty(table)) {
            table = tableRegex;
        } else {
            table = table + "|" +tableRegex;
        }
    }
    
    public void addRowsFilterConfig(RowsFilterConfig config) {
        rowsFilterConfigs.add(config);
    }

    public void addColumnFilterConfig(ColumnsFilterConfig config) {
        columnsFilterConfigs.add(config);
    }
    
    public DrcDbInfo() {
    }

    public DrcDbInfo(String db, String table, String srcMha, String destMha, String srcRegion, String destRegion) {
        this.db = db;
        this.table = table;
        this.srcMha = srcMha;
        this.destMha = destMha;
        this.srcRegion = srcRegion;
        this.destRegion = destRegion;
    }

    @Override
    public String toString() {
        return "DrcDbInfo{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", srcMha='" + srcMha + '\'' +
                ", destMha='" + destMha + '\'' +
                ", srcRegion='" + srcRegion + '\'' +
                ", destRegion='" + destRegion + '\'' +
                '}';
    }
    

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDestMha() {
        return destMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDestRegion() {
        return destRegion;
    }

    public void setDestRegion(String destRegion) {
        this.destRegion = destRegion;
    }


    public List<RowsFilterConfig> getRowsFilterConfigs() {
        return rowsFilterConfigs;
    }

    public void setRowsFilterConfigs(List<RowsFilterConfig> rowsFilterConfigs) {
        this.rowsFilterConfigs = rowsFilterConfigs;
    }

    public List<ColumnsFilterConfig> getColumnsFilterConfigs() {
        return columnsFilterConfigs;
    }

    public void setColumnsFilterConfigs(
            List<ColumnsFilterConfig> columnsFilterConfigs) {
        this.columnsFilterConfigs = columnsFilterConfigs;
    }
}
