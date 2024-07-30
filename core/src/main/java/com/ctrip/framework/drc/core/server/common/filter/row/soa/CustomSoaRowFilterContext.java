package com.ctrip.framework.drc.core.server.common.filter.row.soa;


import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.io.Serializable;


public class CustomSoaRowFilterContext implements  Serializable {
    private static final long serialVersionUID = 1L;
    
    public CustomSoaRowFilterContext(
        String dbName,
        String tableName,
        String columnName,
        String columnValue,
        String srcRegion,
        String dstRegion) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnValue = columnValue;
        this.srcRegion = srcRegion;
        this.dstRegion = dstRegion;
    }

    public CustomSoaRowFilterContext() {
    }

    /**
     * DB名
     */
    private String dbName;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 字段名
     */

    private String columnName;

    /**
     * 字段值
     */
    private String columnValue;

    /**
     * 源端区域（sha/sgp/fra）
     */
    private String srcRegion;

    /**
     * 目标区域（sha/sgp/fra）
     */
    private String dstRegion;
    
    /**
     * DB名
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * DB名
     */
    public void setDbName(final String dbName) {
        this.dbName = dbName;
    }

    /**
     * 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 表名
     */
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    /**
     * 字段名
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 字段名
     */
    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    /**
     * 字段值
     */
    public String getColumnValue() {
        return columnValue;
    }

    /**
     * 字段值
     */
    public void setColumnValue(final String columnValue) {
        this.columnValue = columnValue;
    }

    /**
     * 源端区域（sha/sgp/fra）
     */
    public String getSrcRegion() {
        return srcRegion;
    }

    /**
     * 源端区域（sha/sgp/fra）
     */
    public void setSrcRegion(final String srcRegion) {
        this.srcRegion = srcRegion;
    }

    /**
     * 目标区域（sha/sgp/fra）
     */
    public String getDstRegion() {
        return dstRegion;
    }

    /**
     * 目标区域（sha/sgp/fra）
     */
    public void setDstRegion(final String dstRegion) {
        this.dstRegion = dstRegion;
    }


    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;

        final CustomSoaRowFilterContext other = (CustomSoaRowFilterContext)obj;
        return 
            Objects.equal(this.dbName, other.dbName) &&
            Objects.equal(this.tableName, other.tableName) &&
            Objects.equal(this.columnName, other.columnName) &&
            Objects.equal(this.columnValue, other.columnValue) &&
            Objects.equal(this.srcRegion, other.srcRegion) &&
            Objects.equal(this.dstRegion, other.dstRegion);
    }

    @Override
    public int hashCode() {
        int result = 1;

        result = 31 * result + (this.dbName == null ? 0 : this.dbName.hashCode());
        result = 31 * result + (this.tableName == null ? 0 : this.tableName.hashCode());
        result = 31 * result + (this.columnName == null ? 0 : this.columnName.hashCode());
        result = 31 * result + (this.columnValue == null ? 0 : this.columnValue.hashCode());
        result = 31 * result + (this.srcRegion == null ? 0 : this.srcRegion.hashCode());
        result = 31 * result + (this.dstRegion == null ? 0 : this.dstRegion.hashCode());

        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("dbName", dbName)
            .add("tableName", tableName)
            .add("columnName", columnName)
            .add("columnValue", columnValue)
            .add("srcRegion", srcRegion)
            .add("dstRegion", dstRegion)
            .toString();
    }
}
