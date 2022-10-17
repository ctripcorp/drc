package com.ctrip.framework.drc.core.meta;

import java.util.List;

/**
 * @ClassName MessengerProperties
 * @Author haodongPan
 * @Date 2022/10/9 11:08
 * @Version: $
 */
public class MessengerProperties {
    
    
    private String nameFilter;
    
    private DataMediaConfig dataMediaConfig;
    
    private List<MqConfig> mqConfigs;


    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public void setDataMediaConfig(DataMediaConfig dataMediaConfig) {
        this.dataMediaConfig = dataMediaConfig;
    }

    public List<MqConfig> getMqConfigs() {
        return mqConfigs;
    }

    public void setMqConfigs(List<MqConfig> mqConfigs) {
        this.mqConfigs = mqConfigs;
    }
}
