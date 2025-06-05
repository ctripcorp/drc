package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2025/5/20 11:13
 */
public class IncompatibleMessengerDto {
    private String mhaName;
    List<String> messengerIps;

    public IncompatibleMessengerDto() {
    }

    public IncompatibleMessengerDto(String mhaName, List<String> messengerIps) {
        this.mhaName = mhaName;
        this.messengerIps = messengerIps;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<String> getMessengerIps() {
        return messengerIps;
    }

    public void setMessengerIps(List<String> messengerIps) {
        this.messengerIps = messengerIps;
    }

    @Override
    public String toString() {
        return "IncompatibleMessengerDto{" +
                "mhaName='" + mhaName + '\'' +
                ", messengerIps=" + messengerIps +
                '}';
    }
}
