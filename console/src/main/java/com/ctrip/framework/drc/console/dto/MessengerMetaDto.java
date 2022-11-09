package com.ctrip.framework.drc.console.dto;

import java.util.List;

/**
 * @ClassName MessengerMetaDto
 * @Author haodongPan
 * @Date 2022/11/3 20:33
 * @Version: $
 */
public class MessengerMetaDto {
    
    private String mhaName;
    private List<String> replicatorIps;
    private List<String> messengerIps;
    private String gtidExecuted;

    @Override
    public String toString() {
        return "MessengerMetaDto{" +
                "mhaName='" + mhaName + '\'' +
                ", replicatorIps=" + replicatorIps +
                ", messengerIps=" + messengerIps +
                ", gtidExecuted='" + gtidExecuted + '\'' +
                '}';
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<String> getReplicatorIps() {
        return replicatorIps;
    }

    public void setReplicatorIps(List<String> replicatorIps) {
        this.replicatorIps = replicatorIps;
    }

    public List<String> getMessengerIps() {
        return messengerIps;
    }

    public void setMessengerIps(List<String> messengerIps) {
        this.messengerIps = messengerIps;
    }

    public String getGtidExecuted() {
        return gtidExecuted;
    }

    public void setGtidExecuted(String gtidExecuted) {
        this.gtidExecuted = gtidExecuted;
    }
}
