package com.ctrip.framework.drc.console.enums;

/**
 * @ClassName BroadCastEnum
 * @Author haodongPan
 * @Date 2024/8/9 15:50
 * @Version: $
 */
public enum BroadcastEnum {
    
    MYSQL_MASTER_CHANGE("/api/drc/v1/switch/clusters/dbs/master"),
    REPLICATOR_CHANGE("/api/drc/v1/switch/clusters/replicators/master"),
    MESSENGER_MASTER_CHANGE("/api/drc/v1/switch/clusters/messengers/master"),
    QMQ_DELAY_REFRESH("/api/drc/v2/monitor/refreshQmqDelay"),
    ;
    
    private String path;
    
    
    BroadcastEnum(String path) {
        this.path = path;
    }
    
    public String getPath() {
        return path;
    }
    
 
    

}
