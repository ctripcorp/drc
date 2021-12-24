package com.ctrip.framework.drc.console.pojo;

import java.util.Objects;

/**
 * @Author: hbshen
 * @Date: 2021/4/23
 */
public class MetaKey {

    private String dc;

    private String clusterId;

    private String clusterName;

    private String mhaName;

    public MetaKey(String dc, String clusterId, String clusterName, String mhaName) {
        this.dc = dc;
        this.clusterId = clusterId;
        this.clusterName = clusterName;
        this.mhaName = mhaName;
    }

    private MetaKey(Builder builder) {
        this.dc = builder.dc;
        this.clusterId = builder.clusterId;
        this.clusterName = builder.clusterName;
        this.mhaName = builder.mhaName;
    }

    public static final class Builder {
        private String dc;
        private String clusterId;
        private String clusterName;
        private String mhaName;

        public Builder() {}

        public Builder dc(String val) {
            this.dc = val;
            return this;
        }

        public Builder clusterId(String val) {
            this.clusterId = val;
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

        public MetaKey build() { return new MetaKey(this); }
    }

    public String getDc() {
        return dc;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getMhaName() {
        return mhaName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaKey metaKey = (MetaKey) o;
        return Objects.equals(dc, metaKey.dc) &&
                Objects.equals(clusterId, metaKey.clusterId) &&
                Objects.equals(clusterName, metaKey.clusterName) &&
                Objects.equals(mhaName, metaKey.mhaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dc, clusterId, clusterName, mhaName);
    }

    @Override
    public String toString() {
        return String.format("%s-%s", dc, clusterId);
    }
}