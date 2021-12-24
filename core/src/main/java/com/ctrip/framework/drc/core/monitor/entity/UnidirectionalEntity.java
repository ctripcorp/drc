package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-27
 */
public class UnidirectionalEntity extends BaseEntity {

    private Map<String, String> tags;

    @NotNull(message = "destDcName cannot be null")
    private String destDcName;

    private String destMhaName;

    public UnidirectionalEntity(Builder builder) {
        super(builder.clusterAppId, builder.buName, builder.srcDcName, builder.clusterName, builder.srcMhaName, builder.registryKey);
        this.destDcName = builder.destDcName;
        this.destMhaName = builder.destMhaName;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String srcDcName;
        private String destDcName;
        private String clusterName;
        private String srcMhaName;
        private String destMhaName;
        private String registryKey;

        public Builder() {}

        public Builder clusterAppId(Long val) {
            this.clusterAppId = val;
            return this;
        }

        public Builder buName(String val) {
            this.buName = val;
            return this;
        }

        public Builder srcDcName(String val) {
            this.srcDcName = val;
            return this;
        }

        public Builder destDcName(String val) {
            this.destDcName = val;
            return this;
        }

        public Builder clusterName(String val) {
            this.clusterName = val;
            return this;
        }

        public Builder srcMhaName(String val) {
            this.srcMhaName = val;
            return this;
        }

        public Builder destMhaName(String val) {
            this.destMhaName = val;
            return this;
        }

        public Builder registryKey(String val) {
            this.registryKey = val;
            return this;
        }

        public UnidirectionalEntity build() {
            return new UnidirectionalEntity(this);
        }
    }

    public String getDestDcName() {
        return destDcName;
    }

    public String getDestMhaName() {
        return destMhaName;
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
            String destDc = getDestDcName();
            if(null != destDc) {
                tags.put("destDc", destDc);
            }
            String cluster = getClusterName();
            if(null != cluster) {
                tags.put("cluster", cluster);
            }
            String srcMha = getMhaName();
            if(null != srcMha) {
                tags.put("srcMha", srcMha);
            }
            String destMha = getDestMhaName();
            if(null != destMha) {
                tags.put("destMha", destMha);
            }
            String registryKey = getRegistryKey();
            if(null != registryKey) {
                tags.put("registryKey", registryKey);
            }
        }
        return tags;
    }
}
