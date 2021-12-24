package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-06
 */
public class ConflictEntity extends BaseEndpointEntity {

    private Map<String, String> tags;

    @NotNull(message = "mySQL ip cannot be null")
    private String mysqlIp;

    @NotNull(message = "mySQL port cannot be null")
    private int mysqlPort;

    public ConflictEntity(Builder builder) {
        super(new BaseEndpointEntity.Builder().
                clusterAppId(builder.clusterAppId)
                .buName(builder.buName)
                .dcName(builder.dcName)
                .clusterName(builder.clusterName)
                .mhaName(builder.mha)
                .registryKey(builder.registryKey)
                .ip(builder.applierIp)
                .port(builder.applierPort)
        );
        this.mysqlIp = builder.mysqlIp;
        this.mysqlPort = builder.mysqlPort;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String dcName;
        private String clusterName;
        private String registryKey;
        private String mha;
        private String applierIp;
        private int applierPort;
        private String mysqlIp;
        private int mysqlPort;

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

        public Builder registryKey(String val) {
            this.registryKey = val;
            return this;
        }

        public Builder mha(String val) {
            this.mha = val;
            return this;
        }

        public Builder applierIp(String val) {
            this.applierIp = val;
            return this;
        }

        public Builder applierPort(int val) {
            this.applierPort = val;
            return this;
        }

        public Builder mySqlIp(String val) {
            this.mysqlIp = val;
            return this;
        }

        public Builder mysqlPort(int val) {
            this.mysqlPort = val;
            return this;
        }

        public ConflictEntity build() {
            return new ConflictEntity(this);
        }
    }

    public String getMysqlIp() {
        return mysqlIp;
    }

    public int getMysqlPort() {
        return mysqlPort;
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
                tags.put("applierIp", ip);
            }
            Integer port = getPort();
            if(null != port) {
                tags.put("applierPort", Integer.toString(port));
            }
            String mysqlIp = getMysqlIp();
            if(null != mysqlIp) {
                tags.put("mysqlIp", mysqlIp);
            }
            Integer mysqlPort = getMysqlPort();
            if(null != mysqlPort) {
                tags.put("mysqlPort", Integer.toString(mysqlPort));
            }
        }
        return tags;
    }
}