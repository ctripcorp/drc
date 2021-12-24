package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-30
 */
public class ConsistencyEntity extends BaseEntity {

    private Map<String, String> tags;

    @NotNull(message = "destDcName cannot be null")
    private String destDcName;

    @NotNull(message = "destMhaName cannot be null")
    private String destMhaName;

    @NotNull(message = "src MySQL ip cannot be null")
    private String srcMysqlIp;

    @NotNull(message = "src mySQL port cannot be null")
    private int srcMysqlPort;

    @NotNull(message = "dest mySQL ip cannot be null")
    private String destMysqlIp;

    @NotNull(message = "dest mySQL port cannot be null")
    private int destMysqlPort;

    public ConsistencyEntity(Builder builder) {
        super(builder.clusterAppId, builder.buName, builder.srcDcName, builder.clusterName, builder.mhaName, builder.registryKey);
        this.destDcName = builder.destDcName;
        this.destMhaName = builder.destMhaName;
        this.srcMysqlIp = builder.srcMysqlIp;
        this.srcMysqlPort = builder.srcMysqlPort;
        this.destMysqlIp = builder.destMysqlIp;
        this.destMysqlPort = builder.destMysqlPort;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String srcDcName;
        private String destDcName;
        private String clusterName;
        private String mhaName;
        private String destMhaName;
        private String registryKey;
        private String srcMysqlIp;
        private int srcMysqlPort;
        private String destMysqlIp;
        private int destMysqlPort;

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

        public Builder mhaName(String val) {
            this.mhaName = val;
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

        public Builder srcMysqlIp(String val) {
            this.srcMysqlIp = val;
            return this;
        }

        public Builder srcMysqlPort(int val) {
            this.srcMysqlPort = val;
            return this;
        }

        public Builder destMysqlIp(String val) {
            this.destMysqlIp = val;
            return this;
        }

        public Builder destMysqlPort(int val) {
            this.destMysqlPort = val;
            return this;
        }

        public ConsistencyEntity build() {
            return new ConsistencyEntity(this);
        }
    }

    public String getDestDcName() {
        return destDcName;
    }

    public String getDestMhaName() {
        return destMhaName;
    }

    public String getSrcMysqlIp() {
        return srcMysqlIp;
    }

    public int getSrcMysqlPort() {
        return srcMysqlPort;
    }

    public String getDestMysqlIp() {
        return destMysqlIp;
    }

    public int getDestMysqlPort() {
        return destMysqlPort;
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
            String destDc = getDestDcName();
            if(null != destDc) {
                tags.put("destDc", destDc);
            }
            String destMha = getDestMhaName();
            if(null != destMha) {
                tags.put("destMha", destMha);
            }
            String srcMysqlIp = getSrcMysqlIp();
            if(null != srcMysqlIp) {
                tags.put("srcMysqlIp", srcMysqlIp);
            }
            Integer srcMysqlPort = getSrcMysqlPort();
            if(null != srcMysqlPort) {
                tags.put("srcMysqlPort", Integer.toString(srcMysqlPort));
            }
            String destMysqlIp = getDestMysqlIp();
            if(null != destMysqlIp) {
                tags.put("destMysqlIp", destMysqlIp);
            }
            Integer destMysqlPort = getDestMysqlPort();
            if(null != destMysqlPort) {
                tags.put("destMysqlPort", Integer.toString(destMysqlPort));
            }
        }
        return tags;
    }
}
