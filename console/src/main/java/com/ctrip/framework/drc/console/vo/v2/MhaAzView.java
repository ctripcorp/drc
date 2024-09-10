package com.ctrip.framework.drc.console.vo.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by shiruixin
 * 2024/9/6 15:23
 */
public class MhaAzView {
    Map<String, Set<String>> az2mhaName;
    Map<String, List<String>> az2DbInstance;
    Map<String, List<String>> az2ReplicatorInstance;
    Map<String, List<String>> az2ApplierInstance;


    public Map<String, Set<String>> getAz2mhaName() {
        return az2mhaName;
    }

    public void setAz2mhaName(Map<String, Set<String>> az2mhaName) {
        this.az2mhaName = az2mhaName;
    }

    public Map<String, List<String>> getAz2DbInstance() {
        return az2DbInstance;
    }

    public void setAz2DbInstance(Map<String, List<String>> az2DbInstance) {
        this.az2DbInstance = az2DbInstance;
    }

    public Map<String, List<String>> getAz2ReplicatorInstance() {
        return az2ReplicatorInstance;
    }

    public void setAz2ReplicatorInstance(Map<String, List<String>> az2ReplicatorInstance) {
        this.az2ReplicatorInstance = az2ReplicatorInstance;
    }

    public Map<String, List<String>> getAz2ApplierInstance() {
        return az2ApplierInstance;
    }

    public void setAz2ApplierInstance(Map<String, List<String>> az2ApplierInstance) {
        this.az2ApplierInstance = az2ApplierInstance;
    }
}
