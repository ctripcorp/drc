package com.ctrip.framework.drc.core.monitor.entity;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jixinwang on 2022/9/7
 */
public class TrafficStatisticEntity extends BaseEndpointEntity {

    @NotNull(message = "dbName cannot be null")
    private String dbName;

    @NotNull(message = "srcRegion cannot be null")
    private String srcRegion;

    @NotNull(message = "dstRegion cannot be null")
    private String dstRegion;

    @NotNull(message = "dstType cannot be null")
    private String dstType;

    protected AtomicLong send = new AtomicLong(0);

    public TrafficStatisticEntity(Builder builder) {
        super(new BaseEndpointEntity.Builder()
                .clusterAppId(builder.clusterAppId)
                .buName(builder.buName)
                .dcName(builder.dcName)
                .clusterName(builder.clusterName)
                .mhaName(builder.mha)
                .registryKey(builder.registryKey)
                .ip(builder.mysqlIp)
                .port(builder.mysqlPort)
        );
        this.dbName = builder.dbName;
        this.srcRegion = builder.srcRegion;
        this.dstRegion = builder.dstRegion;
        this.dstType = builder.dstType;
    }

    public static final class Builder {
        private Long clusterAppId;
        private String buName;
        private String dcName;
        private String clusterName;
        private String mysqlIp;
        private int mysqlPort;
        private String dbName;
        private String srcRegion;
        private String dstRegion;
        private String dstType;
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

        public Builder mysqlIp(String val) {
            this.mysqlIp = val;
            return this;
        }

        public Builder mysqlPort(int val) {
            this.mysqlPort = val;
            return this;
        }

        public Builder dbName(String val) {
            this.dbName = val;
            return this;
        }

        public Builder srcRegion(String val) {
            this.srcRegion = val;
            return this;
        }

        public Builder dstRegion(String val) {
            this.dstRegion = val;
            return this;
        }

        public Builder dstType(String val) {
            this.dstType = val;
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

        public TrafficStatisticEntity build() {
            return new TrafficStatisticEntity(this);
        }
    }

    public String getDbName() {
        return dbName;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public String getDstType() {
        return dstType;
    }

    public long getSend() {
        return send.get();
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
            String dbName = getDbName();
            if(null != dbName) {
                tags.put("dbName", dbName);
            }
            String srcRegion = getSrcRegion();
            if(null != srcRegion) {
                tags.put("srcRegion", srcRegion);
            }
            String dstRegion = getDstRegion();
            if(null != dstRegion) {
                tags.put("dstRegion", dstRegion);
            }
            String dstType = getDstType();
            if(null != dstType) {
                tags.put("dstType", dstType);
            }
        }
        return tags;
    }

    public void updateCount(long eventSize) {
        send.addAndGet(eventSize);
    }

    public void clearCount() {
        send.set(0);
    }
}
