package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * mha db mhaReplication which contain replicator or applier from same az
 * Created by dengquanliang
 * 2024/3/19 14:36
 */
public class ResourceSameAzView {

    private List<String> replicatorMhaList;
    private List<String> applierDbList;
    private List<String> messengerMhaList;

    public List<String> getReplicatorMhaList() {
        return replicatorMhaList;
    }

    public void setReplicatorMhaList(List<String> replicatorMhaList) {
        this.replicatorMhaList = replicatorMhaList;
    }

    public List<String> getApplierDbList() {
        return applierDbList;
    }

    public void setApplierDbList(List<String> applierDbList) {
        this.applierDbList = applierDbList;
    }

    public List<String> getMessengerMhaList() {
        return messengerMhaList;
    }

    public void setMessengerMhaList(List<String> messengerMhaList) {
        this.messengerMhaList = messengerMhaList;
    }
}
