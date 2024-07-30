package com.ctrip.framework.drc.service.soa;


import com.ctriposs.baiji.exception.BaijiRuntimeException;
import com.ctriposs.baiji.schema.*;
import com.ctriposs.baiji.specific.*;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.io.Serializable;

@SuppressWarnings("all")
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonPropertyOrder({
        "dbName",
        "tableName",
        "columnName",
        "columnValue",
        "srcRegion",
        "dstRegion",
        "context"
})
public class FilterRowRequestType implements SpecificRecord, Serializable {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    public static final transient Schema SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"FilterRowRequestType\",\"namespace\":\"" + FilterRowRequestType.class.getPackage().getName() + "\",\"doc\":null,\"fields\":[{\"name\":\"dbName\",\"type\":[\"string\",\"null\"]},{\"name\":\"tableName\",\"type\":[\"string\",\"null\"]},{\"name\":\"columnName\",\"type\":[\"string\",\"null\"]},{\"name\":\"columnValue\",\"type\":[\"string\",\"null\"]},{\"name\":\"srcRegion\",\"type\":[\"string\",\"null\"]},{\"name\":\"dstRegion\",\"type\":[\"string\",\"null\"]},{\"name\":\"context\",\"type\":[{\"type\":\"record\",\"name\":\"FilterRowContext\",\"namespace\":\"" + FilterRowContext.class.getPackage().getName() + "\",\"doc\":null,\"fields\":[]},\"null\"]}]}");

    @Override
    @JsonIgnore
    public Schema getSchema() { return SCHEMA; }

    public FilterRowRequestType(
            String dbName,
            String tableName,
            String columnName,
            String columnValue,
            String srcRegion,
            String dstRegion,
            FilterRowContext context) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnValue = columnValue;
        this.srcRegion = srcRegion;
        this.dstRegion = dstRegion;
        this.context = context;
    }

    public FilterRowRequestType() {
    }

    /**
     * DB名
     */
    @JsonProperty("dbName")
    private String dbName;

    /**
     * 表名
     */
    @JsonProperty("tableName")
    private String tableName;

    /**
     * 字段名
     */
    @JsonProperty("columnName")
    private String columnName;

    /**
     * 字段值
     */
    @JsonProperty("columnValue")
    private String columnValue;

    /**
     * 源端区域（sha/sgp/fra）
     */
    @JsonProperty("srcRegion")
    private String srcRegion;

    /**
     * 目标区域（sha/sgp/fra）
     */
    @JsonProperty("dstRegion")
    private String dstRegion;

    @JsonProperty("context")
    private FilterRowContext context;

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
    public FilterRowContext getContext() {
        return context;
    }

    public void setContext(final FilterRowContext context) {
        this.context = context;
    }

    // Used by DatumWriter. Applications should not call.
    public Object get(int fieldPos) {
        switch (fieldPos) {
            case 0: return this.dbName;
            case 1: return this.tableName;
            case 2: return this.columnName;
            case 3: return this.columnValue;
            case 4: return this.srcRegion;
            case 5: return this.dstRegion;
            case 6: return this.context;
            default: throw new BaijiRuntimeException("Bad index " + fieldPos + " in get()");
        }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value="unchecked")
    public void put(int fieldPos, Object fieldValue) {
        switch (fieldPos) {
            case 0: this.dbName = (String)fieldValue; break;
            case 1: this.tableName = (String)fieldValue; break;
            case 2: this.columnName = (String)fieldValue; break;
            case 3: this.columnValue = (String)fieldValue; break;
            case 4: this.srcRegion = (String)fieldValue; break;
            case 5: this.dstRegion = (String)fieldValue; break;
            case 6: this.context = (FilterRowContext)fieldValue; break;
            default: throw new BaijiRuntimeException("Bad index " + fieldPos + " in put()");
        }
    }

    @Override
    public Object get(String fieldName) {
        Schema schema = getSchema();
        if (!(schema instanceof RecordSchema)) {
            return null;
        }
        Field field = ((RecordSchema) schema).getField(fieldName);
        return field != null ? get(field.getPos()) : null;
    }

    @Override
    public void put(String fieldName, Object fieldValue) {
        Schema schema = getSchema();
        if (!(schema instanceof RecordSchema)) {
            return;
        }
        Field field = ((RecordSchema) schema).getField(fieldName);
        if (field != null) {
            put(field.getPos(), fieldValue);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;

        final FilterRowRequestType other = (FilterRowRequestType)obj;
        return
                Objects.equal(this.dbName, other.dbName) &&
                        Objects.equal(this.tableName, other.tableName) &&
                        Objects.equal(this.columnName, other.columnName) &&
                        Objects.equal(this.columnValue, other.columnValue) &&
                        Objects.equal(this.srcRegion, other.srcRegion) &&
                        Objects.equal(this.dstRegion, other.dstRegion) &&
                        Objects.equal(this.context, other.context);
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
        result = 31 * result + (this.context == null ? 0 : this.context.hashCode());

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
                .add("context", context)
                .toString();
    }
}
