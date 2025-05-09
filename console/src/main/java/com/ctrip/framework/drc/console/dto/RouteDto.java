package com.ctrip.framework.drc.console.dto;

import java.util.List;

public class RouteDto {

    private Long id;

    private String routeOrgName;

    private String srcDcName;
    
    private String srcRegionName;

    private String dstDcName;
    
    private String dstRegionName;

    private List<String> srcProxyUris;

    private List<String> relayProxyUris;

    private List<String> dstProxyUris;

    private String tag;
    
    private Integer deleted;

    private int globalActive;

    private Long relatedNum;

    public Long getRelatedNum() {
        return relatedNum;
    }

    public void setRelatedNum(Long relatedNum) {
        this.relatedNum = relatedNum;
    }

    public int getGlobalActive() {
        return globalActive;
    }

    public void setGlobalActive(int globalActive) {
        this.globalActive = globalActive;
    }

    public RouteDto() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getRouteOrgName() {
        return routeOrgName;
    }

    public void setRouteOrgName(String routeOrgName) {
        this.routeOrgName = routeOrgName;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public void setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
    }

    public List<String> getSrcProxyUris() {
        return srcProxyUris;
    }

    public void setSrcProxyUris(List<String> srcProxyUris) {
        this.srcProxyUris = srcProxyUris;
    }

    public List<String> getRelayProxyUris() {
        return relayProxyUris;
    }

    public void setRelayProxyUris(List<String> relayProxyUris) {
        this.relayProxyUris = relayProxyUris;
    }

    public List<String> getDstProxyUris() {
        return dstProxyUris;
    }

    public void setDstProxyUris(List<String> dstProxyUris) {
        this.dstProxyUris = dstProxyUris;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }


    public String getSrcRegionName() {
        return srcRegionName;
    }

    public void setSrcRegionName(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }

    public String getDstRegionName() {
        return dstRegionName;
    }

    public void setDstRegionName(String dstRegionName) {
        this.dstRegionName = dstRegionName;
    }

    @Override
    public String toString() {
        return "RouteDto{" +
                "id=" + id +
                ", routeOrgName='" + routeOrgName + '\'' +
                ", srcDcName='" + srcDcName + '\'' +
                ", srcRegionName='" + srcRegionName + '\'' +
                ", dstDcName='" + dstDcName + '\'' +
                ", dstRegionName='" + dstRegionName + '\'' +
                ", srcProxyUris=" + srcProxyUris +
                ", relayProxyUris=" + relayProxyUris +
                ", dstProxyUris=" + dstProxyUris +
                ", tag='" + tag + '\'' +
                ", deleted=" + deleted +
                '}';
    }
}
