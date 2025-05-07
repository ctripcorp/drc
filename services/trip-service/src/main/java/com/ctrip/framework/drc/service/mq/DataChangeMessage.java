package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

/**
 * @ClassName DataChangeMessage
 * @Author haodongPan
 * @Date 2022/11/9 11:58
 * @Version: $
 */
public class DataChangeMessage {
    @JSONField(ordinal = 1)
    private OrderKeyInfo orderKeyInfo;
    @JSONField(ordinal = 5)
    private String eventType;
    @JSONField(ordinal = 6)
    private String schemaName;
    @JSONField(ordinal = 9)
    private String tableName;
    @JSONField(ordinal = 7)
    private Long drcSendTime;
    @JSONField(ordinal = 2)
    private long otterParseTime;
    @JSONField(ordinal = 3)
    private long otterSendTime;
    @JSONField(ordinal = 4)
    private List<ColumnData> beforeColumnList;
    @JSONField(ordinal = 8)
    private List<ColumnData> afterColumnList;

    public OrderKeyInfo getOrderKeyInfo() {
        return orderKeyInfo;
    }

    public void setOrderKeyInfo(OrderKeyInfo orderKeyInfo) {
        this.orderKeyInfo = orderKeyInfo;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getDrcSendTime() {
        return drcSendTime;
    }

    public void setDrcSendTime(Long drcSendTime) {
        this.drcSendTime = drcSendTime;
    }

    public long getOtterParseTime() {
        return otterParseTime;
    }

    public void setOtterParseTime(long otterParseTime) {
        this.otterParseTime = otterParseTime;
    }

    public long getOtterSendTime() {
        return otterSendTime;
    }

    public void setOtterSendTime(long otterSendTime) {
        this.otterSendTime = otterSendTime;
    }

    public List<ColumnData> getBeforeColumnList() {
        return beforeColumnList;
    }

    public void setBeforeColumnList(List<ColumnData> beforeColumnList) {
        this.beforeColumnList = beforeColumnList;
    }

    public List<ColumnData> getAfterColumnList() {
        return afterColumnList;
    }

    public void setAfterColumnList(List<ColumnData> afterColumnList) {
        this.afterColumnList = afterColumnList;
    }

    public static final class OrderKeyInfo {
        @JSONField(name = "pks")
        private List<String> pk;
        private String schemaName;
        private String tableName;

        public List<String> getPk() {
            return pk;
        }

        public void setPk(List<String> pk) {
            this.pk = pk;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
    }
    
    public static final class ColumnData {
        @JSONField(name = "isNull",ordinal = 1)
        private boolean isNull;
        @JSONField(ordinal = 2)
        private String name;
        @JSONField(name = "isKey",ordinal = 3)
        private boolean isKey;
        @JSONField(name = "isUpdated",ordinal = 4)
        private boolean isUpdated;
        @JSONField(ordinal = 5)
        private String value;

        public ColumnData(String var1, String var2, Boolean var3, Boolean var4, Boolean var5) {
            this.name = var1;
            this.value = var2;
            this.isUpdated = var3;
            this.isKey = var4;
            this.isNull = var5;
        }

        public boolean isNull() {
            return isNull;
        }

        public void setNull(boolean aNull) {
            isNull = aNull;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isKey() {
            return isKey;
        }

        public void setKey(boolean key) {
            isKey = key;
        }

        public boolean isUpdated() {
            return isUpdated;
        }

        public void setUpdated(boolean updated) {
            isUpdated = updated;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
