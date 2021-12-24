package com.ctrip.framework.drc.manager.ha.rest;

import com.ctrip.xpipe.rest.ForwardType;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class ForwardInfo implements Cloneable{

    private ForwardType type;

    private List<String> forwardServers = new LinkedList<>();

    public ForwardInfo(){

    }

    public ForwardInfo(ForwardType type){
        this(type, null);
    }

    public ForwardInfo(ForwardType type, String forwardServer){

        this.type = type;
        if(forwardServer != null){
            this.forwardServers.add(forwardServer);
        }
    }

    public boolean hasServer(String serverId){

        for(String passServerId : forwardServers){
            if(passServerId.equals(serverId)){
                return true;
            }
        }
        return false;
    }


    public ForwardType getType() {
        return type;
    }

    public void setType(ForwardType type) {
        this.type = type;
    }

    public List<String> getForwardServers() {
        return forwardServers;
    }

    public void setForwardServers(List<String> forwardServers) {
        this.forwardServers = forwardServers;
    }

    public void addForwardServers(String server){
        forwardServers.add(server);
    }

    @Override
    public String toString() {
        return String.format("type:%s, fromServers:%s", type, forwardServers);
    }

    @Override
    public ForwardInfo clone() {

        ForwardInfo forwardInfo;
        try {
            forwardInfo = (ForwardInfo) super.clone();
            forwardInfo.forwardServers = new LinkedList<>(forwardServers);
            return forwardInfo;
        } catch (CloneNotSupportedException e) {
        }
        return null;
    }
}
