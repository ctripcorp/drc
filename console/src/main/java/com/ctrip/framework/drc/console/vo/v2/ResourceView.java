package com.ctrip.framework.drc.console.vo.v2;

import java.util.Objects;

/**
 * Created by dengquanliang
 * 2023/8/3 17:56
 */
public class ResourceView implements Comparable<ResourceView> {
    private Long resourceId;
    private String ip;
    private Integer active;
    private String az;
    private Integer type;
    private Long instanceNum;
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getInstanceNum() {
        return instanceNum;
    }

    public void setInstanceNum(Long instanceNum) {
        this.instanceNum = instanceNum;
    }

    public String getAz() {
        return az;
    }

    public void setAz(String az) {
        this.az = az;
    }

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getActive() {
        return active;
    }

    public void setActive(Integer active) {
        this.active = active;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceView view = (ResourceView) o;
        return Objects.equals(ip, view.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip);
    }

    @Override
    public int compareTo(ResourceView o) {
        return this.instanceNum.compareTo(o.getInstanceNum());
    }
}
