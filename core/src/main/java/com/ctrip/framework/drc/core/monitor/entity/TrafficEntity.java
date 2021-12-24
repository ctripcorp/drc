package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-10
 */
public class TrafficEntity extends BaseEndpointEntity {

    private Map<String, String> tags;

    @NotNull(message = "module cannot be null")
    private String module;

    @NotNull(message = "direction cannot be null")
    private String direction;

    public TrafficEntity(Builder builder) {
        super(new BaseEndpointEntity.Builder().
                clusterAppId(builder.clusterAppId)
                .buName(builder.buName)
                .dcName(builder.dcName)
                .clusterName(builder.clusterName)
                .mhaName(builder.mha)
                .registryKey(builder.registryKey)
                .ip(builder.ip)
                .port(builder.port)
        );
        this.module = builder.module;
        this.direction = builder.direction;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String dcName;
        private String clusterName;
        private String ip;
        private int port;
        private String module;
        private String direction;
        private String mha;
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

        public Builder dcName(String val) {
            this.dcName = val;
            return this;
        }

        public Builder clusterName(String val) {
            this.clusterName = val;
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

        public Builder module(String val) {
            this.module = val;
            return this;
        }

        public Builder direction(String val) {
            this.direction = val;
            return this;
        }

        public Builder mha(String val) {
            this.mha = val;
            return this;
        }

        public Builder registryKey(String val) {
            this.registryKey = val;
            return this;
        }

        public TrafficEntity build() {
            return new TrafficEntity(this);
        }
    }

    public String getModule() {
        return module;
    }

    public String getDirection() {
        return direction;
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
                tags.put("dc", dc);
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
            String direction = getDirection();
            if(null != direction) {
                tags.put("direction", getDirection());
            }
            String module = getModule();
            if(null != module) {
                tags.put("module", getModule());
            }
        }
        return tags;
    }
}
