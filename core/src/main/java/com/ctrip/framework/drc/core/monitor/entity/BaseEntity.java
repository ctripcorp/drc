package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-05
 */
public class BaseEntity {

    private Map<String, String> tags;

    private Long clusterAppId;

    @NotNull(message = "buName cannot be null")
    private String buName;

    @NotNull(message = "dcName cannot be null")
    private String dcName;

    @NotNull(message = "clusterName cannot be null")
    private String clusterName;

    @NotNull(message = "mhaName cannot be null")
    private String mhaName;

    private String registryKey;

    public Long getClusterAppId() {
        return clusterAppId;
    }

    public String getBuName() {
        return buName;
    }

    public String getDcName() {
        return dcName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getMhaName() {
        return mhaName;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public BaseEntity(Long clusterAppId, String buName, String dcName, String clusterName, String mhaName, String registryKey) {
        this.clusterAppId = clusterAppId;
        this.buName = buName;
        this.dcName = dcName;
        this.clusterName = clusterName;
        this.mhaName = mhaName;
        this.registryKey = registryKey;
    }

    public Map<String, String> getTags() {
        if(null == tags) {
            tags = new HashMap<>();
            Long clusterAppId = getClusterAppId();
            if(null != clusterAppId) {
                tags.put("clusterAppId", Long.toString(clusterAppId));
            }
            String bu = getBuName();
            if(null != bu) {
                tags.put("bu", bu);
            }
            String dc = getDcName();
            if(null != dc) {
                tags.put("srcDc", dc);
            }
            String cluster = getClusterName();
            if(null != cluster) {
                tags.put("cluster", cluster);
            }
            String mha = getMhaName();
            if(null != mha) {
                tags.put("mha", mha);
            }
            String registryKey = getRegistryKey();
            if(null != registryKey) {
                tags.put("registryKey", registryKey);
            }
        }
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseEntity)) return false;
        BaseEntity that = (BaseEntity) o;
        return Objects.equals(getTags(), that.getTags()) &&
                Objects.equals(getClusterAppId(), that.getClusterAppId()) &&
                Objects.equals(getBuName(), that.getBuName()) &&
                Objects.equals(getDcName(), that.getDcName()) &&
                Objects.equals(getClusterName(), that.getClusterName()) &&
                Objects.equals(getMhaName(), that.getMhaName()) &&
                Objects.equals(getRegistryKey(), that.getRegistryKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTags(), getClusterAppId(), getBuName(), getDcName(), getClusterName(), getMhaName(), getRegistryKey());
    }
}
