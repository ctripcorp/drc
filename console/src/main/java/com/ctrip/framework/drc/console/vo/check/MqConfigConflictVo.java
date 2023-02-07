package com.ctrip.framework.drc.console.vo.check;

/**
 * @ClassName MqConfigConflictVo
 * @Author haodongPan
 * @Date 2023/2/7 15:52
 * @Version: $
 */
public class MqConfigConflictVo {
    
    private String table;
    
    private String topic;
    
    private String tag;

    @Override
    public String toString() {
        return "MqConfigConflictVo{" +
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
