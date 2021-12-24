package com.ctrip.framework.drc.console.dto;

import java.util.List;

/**
 * Created by jixinwang on 2020/11/17
 */
public class DalMhaDto {

    private String mhaName;

    private List<DalClusterDto> clusters;

    public DalMhaDto(String mhaName, List<DalClusterDto> clusters) {
        this.mhaName = mhaName;
        this.clusters = clusters;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<DalClusterDto> getClusters() {
        return clusters;
    }

    public void setClusters(List<DalClusterDto> clusters) {
        this.clusters = clusters;
    }
}
