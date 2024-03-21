package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-27
 */
public class GtidGapEntity extends BaseEndpointEntity {

    private Map<String, String> tags;

    @NotNull(message = "uuid cannot be null")
    private String uuid;

    public GtidGapEntity(Builder builder) {
        super(new BaseEndpointEntity.Builder().
                clusterAppId(builder.clusterAppId)
                .buName(builder.buName)
                .dcName(builder.dcName)
                .clusterName(builder.clusterName)
                .mhaName(builder.mha)
                .dbName(builder.db)
                .registryKey(builder.registryKey)
                .ip(builder.mysqlIp)
                .port(builder.mysqlPort)
        );
        this.uuid = builder.uuid;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String dcName;
        private String clusterName;
        private String mysqlIp;
        private int mysqlPort;
        private String uuid;
        private String mha;
        private String registryKey;
        private String db;

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

        public Builder mysqlIp(String val) {
            this.mysqlIp = val;
            return this;
        }

        public Builder mysqlPort(int val) {
            this.mysqlPort = val;
            return this;
        }

        public Builder uuid(String val) {
            this.uuid = val;
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
        public Builder db(String val) {
            this.db = val;
            return this;
        }

        public GtidGapEntity build() {
            return new GtidGapEntity(this);
        }
    }

    public String getUuid() {
        return uuid;
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
            String uuid = getUuid();
            if(null != uuid) {
                tags.put("uuid", uuid);
            }
            String db = getDbName();
            if(null != db) {
                tags.put("db", db);
            }
        }
        return tags;
    }
}
