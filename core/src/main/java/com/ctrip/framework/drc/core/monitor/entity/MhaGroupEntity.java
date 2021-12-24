package com.ctrip.framework.drc.core.monitor.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-06-28
 */
public class MhaGroupEntity extends BaseEntity {

    private Map<String, String> tags;

    private String mhaGroupKey;

    public MhaGroupEntity(Builder builder) {
        super(builder.clusterAppId, builder.buName, builder.dcName, builder.clusterName, builder.mhaName, builder.registryKey);
        this.mhaGroupKey = builder.mhaGroupKey;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String dcName;
        private String clusterName;
        private String mhaName;
        private String registryKey;
        private String mhaGroupKey;

        public Builder() {}

        public Builder clusterAppId(Long val) {
            this.clusterAppId = val;
            return this;
        }

        public Builder buName(String val) {
            this.buName = val;
            return this;
        }

        public Builder dcName(String val) {
            this.dcName = val;
            return this;
        }

        public Builder clusterName(String val) {
            this.clusterName = val;
            return this;
        }

        public Builder mhaName(String val) {
            this.mhaName = val;
            return this;
        }

        public Builder registryKey(String val) {
            this.registryKey = val;
            return this;
        }

        public Builder mhaGroupKey(String val) {
            this.mhaGroupKey = val;
            return this;
        }

        public MhaGroupEntity build() {
            return new MhaGroupEntity(this);
        }
    }

    public String getMhaGroupKey() {
        return mhaGroupKey;
    }



    @Override
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
            String mhaGroupKey = getMhaGroupKey();
            if(null != mhaGroupKey) {
                tags.put("mhaGroupKey", mhaGroupKey);
            }
        }
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaGroupEntity)) return false;
        if (!super.equals(o)) return false;
        MhaGroupEntity that = (MhaGroupEntity) o;
        return Objects.equals(getTags(), that.getTags()) &&
                Objects.equals(getMhaGroupKey(), that.getMhaGroupKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getTags(), getMhaGroupKey());
    }
}
