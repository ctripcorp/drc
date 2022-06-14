package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-07
 */
public class BaseEndpointEntity extends BaseEntity {

    protected Map<String, String> tags;

    @NotNull(message = "ip cannot be null")
    private String ip;

    @NotNull(message = "port cannot be null")
    private int port;

    public BaseEndpointEntity(Builder builder) {
        super(builder.clusterAppId, builder.buName, builder.dcName, builder.clusterName, builder.mhaName, builder.registryKey);
        this.ip = builder.ip;
        this.port = builder.port;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String dcName;
        private String clusterName;
        private String mhaName;
        private String registryKey;
        private String ip;
        private int port;

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

        public Builder ip(String val) {
            this.ip = val;
            return this;
        }

        public Builder port(int val) {
            this.port = val;
            return this;
        }

        public BaseEndpointEntity build() {
            return new BaseEndpointEntity(this);
        }
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
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
            String ip = getIp();
            if(null != ip) {
                tags.put("ip", ip);
            }
            Integer port = getPort();
            if(null != port) {
                tags.put("port", Integer.toString(port));
            }
        }
        return tags;
    }
}
