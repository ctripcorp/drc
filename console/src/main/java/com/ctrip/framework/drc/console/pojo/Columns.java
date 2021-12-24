package com.ctrip.framework.drc.console.pojo;

import java.math.BigInteger;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-25
 */
public class Columns {

    /**
     * `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''
     * value is always 'def'
     */
    private String tableCatalog;
    /**
     * `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',
     * database/schema which the table belongs to
     */
    private String tableSchema;
    /**
     * `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',
     */
    private String tableName;
    /**
     * `COLUMN_NAME` varchar(64) NOT NULL DEFAULT '',
     */
    private String columnName;
    /**
     * `ORDINAL_POSITION` bigint(21) unsigned NOT NULL DEFAULT '0',
     * id of the column which starts from 1
     */
    private BigInteger ordinalPosition;
    /**
     * `COLUMN_DEFAULT` longtext,
     * default column value
     */
    private String columnDefault;
    /**
     * `IS_NULLABLE` varchar(3) NOT NULL DEFAULT '',
     * whether the column value could be null, value can only be: YES, NO
     */
    private String isNullable;
    /**
     * `DATA_TYPE` varchar(64) NOT NULL DEFAULT '',
     */
    private String dataType;
    /**
     * `CHARACTER_MAXIMUM_LENGTH` bigint(21) unsigned DEFAULT NULL,
     * maximum
     */
    private BigInteger characterMaximumLength;
    /**
     * `CHARACTER_OCTET_LENGTH` bigint(21) unsigned DEFAULT NULL,
     */
    private BigInteger characterOctetLength;
    /**
     * `NUMERIC_PRECISION` bigint(21) unsigned DEFAULT NULL,
     */
    private BigInteger numericPrecision;
    /**
     * `NUMERIC_SCALE` bigint(21) unsigned DEFAULT NULL,
     */
    private BigInteger numericScale;
    /**
     * `DATETIME_PRECISION` bigint(21) unsigned DEFAULT NULL,
     */
    private BigInteger datetimePrecision;
    /**
     * `CHARACTER_SET_NAME` varchar(32) DEFAULT NULL,
     * column charset name, for example: utf8
     */
    private String characterSetName;
    /**
     * `COLLATION_NAME` varchar(32) DEFAULT NULL,
     * column charset ordering/collating rule
     */
    private String collationName;
    /**
     * `COLUMN_TYPE` longtext NOT NULL,
     */
    private String columnType;
    /**
     * `COLUMN_KEY` varchar(3) NOT NULL DEFAULT '',
     * index type of column, the value could be: PRI, UNI, MUL
     */
    private String columnKey;
    /**
     * `EXTRA` varchar(30) NOT NULL DEFAULT '',
     * other info like "auto_increment" for the primary key
     */
    private String extra;
    /**
     * `PRIVILEGES` varchar(80) NOT NULL DEFAULT '',
     * privileges separated by comma, for example: select,insert,update,references
     */
    private String privileges;
    /**
     * `COLUMN_COMMENT` varchar(1024) NOT NULL DEFAULT '',
     */
    private String columnComment;
    /**
     * `GENERATION_EXPRESSION` longtext NOT NULL
     */
    private String generationExpression;

    private Columns(String tableCatalog, String tableSchema, String tableName, String columnName, BigInteger ordinalPosition, String columnDefault, String isNullable, String dataType, BigInteger characterMaximumLength, BigInteger characterOctetLength, BigInteger numericPrecision, BigInteger numericScale, BigInteger datetimePrecision, String characterSetName, String collationName, String columnType, String columnKey, String extra, String privileges, String columnComment, String generationExpression) {
        this.tableCatalog = tableCatalog;
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.columnName = columnName;
        this.ordinalPosition = ordinalPosition;
        this.columnDefault = columnDefault;
        this.isNullable = isNullable;
        this.dataType = dataType;
        this.characterMaximumLength = characterMaximumLength;
        this.characterOctetLength = characterOctetLength;
        this.numericPrecision = numericPrecision;
        this.numericScale = numericScale;
        this.datetimePrecision = datetimePrecision;
        this.characterSetName = characterSetName;
        this.collationName = collationName;
        this.columnType = columnType;
        this.columnKey = columnKey;
        this.extra = extra;
        this.privileges = privileges;
        this.columnComment = columnComment;
        this.generationExpression = generationExpression;
    }

    public static final class Builder {
        private String tableCatalog;
        private String tableSchema;
        private String tableName;
        private String columnName;
        private BigInteger ordinalPosition;
        private String columnDefault;
        private String isNullable;
        private String dataType;
        private BigInteger characterMaximumLength;
        private BigInteger characterOctetLength;
        private BigInteger numericPrecision;
        private BigInteger numericScale;
        private BigInteger datetimePrecision;
        private String characterSetName;
        private String collationName;
        private String columnType;
        private String columnKey;
        private String extra;
        private String privileges;
        private String columnComment;
        private String generationExpression;

        public Builder() {
        }

        public Builder tableCatalog(String val) {
            this.tableCatalog = val;
            return this;
        }

        public Builder tableSchema(String val) {
            this.tableSchema = val;
            return this;
        }

        public Builder tableName(String val) {
            this.tableName = val;
            return this;
        }

        public Builder columnName(String val) {
            this.columnName = val;
            return this;
        }

        public Builder ordinalPosition(BigInteger val) {
            this.ordinalPosition = val;
            return this;
        }

        public Builder columnDefault(String val) {
            this.columnDefault = val;
            return this;
        }

        public Builder isNullable(String val) {
            this.isNullable = val;
            return this;
        }

        public Builder dataType(String val) {
            this.dataType = val;
            return this;
        }

        public Builder characterMaximumLength(BigInteger val) {
            this.characterMaximumLength = val;
            return this;
        }

        public Builder characterOctetLength(BigInteger val) {
            this.characterOctetLength = val;
            return this;
        }

        public Builder numericPrecision(BigInteger val) {
            this.numericPrecision = val;
            return this;
        }

        public Builder numericScale(BigInteger val) {
            this.numericScale = val;
            return this;
        }

        public Builder datetimePrecision(BigInteger val) {
            this.datetimePrecision = val;
            return this;
        }

        public Builder characterSetName(String val) {
            this.characterSetName = val;
            return this;
        }

        public Builder collationName(String val) {
            this.collationName = val;
            return this;
        }

        public Builder columnType(String val) {
            this.columnType = val;
            return this;
        }

        public Builder columnKey(String val) {
            this.columnKey = val;
            return this;
        }

        public Builder extra(String val) {
            this.extra = val;
            return this;
        }

        public Builder privileges(String val) {
            this.privileges = val;
            return this;
        }

        public Builder columnComment(String val) {
            this.columnComment = val;
            return this;
        }

        public Builder generationExpression(String val) {
            this.generationExpression = val;
            return this;
        }

        public Columns build() {
            return new Columns(tableCatalog, tableSchema, tableName, columnName, ordinalPosition, columnDefault, isNullable, dataType, characterMaximumLength, characterOctetLength, numericPrecision, numericScale, datetimePrecision, characterSetName, collationName, columnType, columnKey, extra, privileges, columnComment, generationExpression);
        }
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public BigInteger getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getColumnDefault() {
        return columnDefault;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public String getDataType() {
        return dataType;
    }

    public BigInteger getCharacterMaximumLength() {
        return characterMaximumLength;
    }

    public BigInteger getCharacterOctetLength() {
        return characterOctetLength;
    }

    public BigInteger getNumericPrecision() {
        return numericPrecision;
    }

    public BigInteger getNumericScale() {
        return numericScale;
    }

    public BigInteger getDatetimePrecision() {
        return datetimePrecision;
    }

    public String getCharacterSetName() {
        return characterSetName;
    }

    public String getCollationName() {
        return collationName;
    }

    public String getColumnType() {
        return columnType;
    }

    public String getColumnKey() {
        return columnKey;
    }

    public String getExtra() {
        return extra;
    }

    public String getPrivileges() {
        return privileges;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public String getGenerationExpression() {
        return generationExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Columns)) return false;
        Columns columns = (Columns) o;
        return Objects.equals(tableCatalog, columns.tableCatalog) &&
                Objects.equals(tableSchema, columns.tableSchema) &&
                Objects.equals(tableName, columns.tableName) &&
                Objects.equals(columnName, columns.columnName) &&
                Objects.equals(ordinalPosition, columns.ordinalPosition) &&
                Objects.equals(columnDefault, columns.columnDefault) &&
                Objects.equals(isNullable, columns.isNullable) &&
                Objects.equals(dataType, columns.dataType) &&
                Objects.equals(characterMaximumLength, columns.characterMaximumLength) &&
                Objects.equals(characterOctetLength, columns.characterOctetLength) &&
                Objects.equals(numericPrecision, columns.numericPrecision) &&
                Objects.equals(numericScale, columns.numericScale) &&
                Objects.equals(datetimePrecision, columns.datetimePrecision) &&
                Objects.equals(characterSetName, columns.characterSetName) &&
                Objects.equals(collationName, columns.collationName) &&
                Objects.equals(columnType, columns.columnType) &&
                Objects.equals(columnKey, columns.columnKey) &&
                Objects.equals(extra, columns.extra) &&
                Objects.equals(privileges, columns.privileges) &&
                Objects.equals(columnComment, columns.columnComment) &&
                Objects.equals(generationExpression, columns.generationExpression);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tableCatalog, tableSchema, tableName, columnName, ordinalPosition, columnDefault, isNullable, dataType, characterMaximumLength, characterOctetLength, numericPrecision, numericScale, datetimePrecision, characterSetName, collationName, columnType, columnKey, extra, privileges, columnComment, generationExpression);
    }

    @Override
    public String toString() {
        return "Columns{" +
                "tableCatalog='" + tableCatalog + '\'' +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", ordinalPosition=" + ordinalPosition +
                ", columnDefault='" + columnDefault + '\'' +
                ", isNullable='" + isNullable + '\'' +
                ", dataType='" + dataType + '\'' +
                ", characterMaximumLength=" + characterMaximumLength +
                ", characterOctetLength=" + characterOctetLength +
                ", numericPrecision=" + numericPrecision +
                ", numericScale=" + numericScale +
                ", datetimePrecision=" + datetimePrecision +
                ", characterSetName='" + characterSetName + '\'' +
                ", collationName='" + collationName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", columnKey='" + columnKey + '\'' +
                ", extra='" + extra + '\'' +
                ", privileges='" + privileges + '\'' +
                ", columnComment='" + columnComment + '\'' +
                ", generationExpression='" + generationExpression + '\'' +
                '}';
    }
}
