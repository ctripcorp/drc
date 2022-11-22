package com.ctrip.framework.drc.service.mq;

import java.util.List;

/**
 * @ClassName DataChangeMessage
 * @Author haodongPan
 * @Date 2022/11/9 11:58
 * @Version: $
 */
public class DataChangeMessage {
    
    private OrderKeyInfo orderKeyInfo;
    private String eventType;
    private String schemaName;
    private String tableName;
    private String drcSendTime;
    private long otterParseTime;
    private long otterSendTime;
    
    private List<ColumnData> beforeColumnList;
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

    public String getDrcSendTime() {
        return drcSendTime;
    }

    public void setDrcSendTime(String drcSendTime) {
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
        
        private boolean isNull;
        private String name;
        private boolean isKey;
        private boolean isUpdated;
        private String value;

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
