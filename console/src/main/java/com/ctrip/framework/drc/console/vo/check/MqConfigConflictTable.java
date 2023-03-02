package com.ctrip.framework.drc.console.vo.check;

/**
 * @ClassName MqConfigConflictTable
 * @Author haodongPan
 * @Date 2023/2/7 15:52
 * @Version: $
 */
public class MqConfigConflictTable {
    
    private String table;
    
    private String topic;
    
    private String tag;

    @Override
    public String toString() {
        return "MqConfigConflictTable{" +
                "table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
