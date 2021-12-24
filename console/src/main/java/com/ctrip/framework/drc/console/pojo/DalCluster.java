package com.ctrip.framework.drc.console.pojo;

import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-08
 */
public class DalCluster {

    //{"clusterName":"testClusterName","type":"testType","unitStrategyId":2,"unitStrategyName":"testStrategyName","shardStrategies":null,"idGenerators":null,"dbCategory":"mysql","enabled":true,"released":true,"releaseVersion":23,"shards":null}

    private String clusterName;

    private String type;

    private long unitStrategyId;

    private String unitStrategyName;

    private String shardStrategies;

    private String idGenerators;

    private String dbCategory;

    private boolean enabled;

    private boolean released;

    private int releaseVersion;

    private String shards;

    public DalCluster() {
    }

    public DalCluster(String clusterName, String type, long unitStrategyId, String unitStrategyName, String shardStrategies, String idGenerators, String dbCategory, boolean enabled, boolean released, int releaseVersion, String shards) {
        this.clusterName = clusterName;
        this.type = type;
        this.unitStrategyId = unitStrategyId;
        this.unitStrategyName = unitStrategyName;
        this.shardStrategies = shardStrategies;
        this.idGenerators = idGenerators;
        this.dbCategory = dbCategory;
        this.enabled = enabled;
        this.released = released;
        this.releaseVersion = releaseVersion;
        this.shards = shards;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getType() {
        return type;
    }

    public long getUnitStrategyId() {
        return unitStrategyId;
    }

    public String getUnitStrategyName() {
        return unitStrategyName;
    }

    public String getShardStrategies() {
        return shardStrategies;
    }

    public String getIdGenerators() {
        return idGenerators;
    }

    public String getDbCategory() {
        return dbCategory;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isReleased() {
        return released;
    }

    public int getReleaseVersion() {
        return releaseVersion;
    }

    public String getShards() {
        return shards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DalCluster)) return false;
        DalCluster that = (DalCluster) o;
        return getUnitStrategyId() == that.getUnitStrategyId() &&
                isEnabled() == that.isEnabled() &&
                isReleased() == that.isReleased() &&
                getReleaseVersion() == that.getReleaseVersion() &&
                Objects.equals(getClusterName(), that.getClusterName()) &&
                Objects.equals(getType(), that.getType()) &&
                Objects.equals(getUnitStrategyName(), that.getUnitStrategyName()) &&
                Objects.equals(getShardStrategies(), that.getShardStrategies()) &&
                Objects.equals(getIdGenerators(), that.getIdGenerators()) &&
                Objects.equals(getDbCategory(), that.getDbCategory()) &&
                Objects.equals(getShards(), that.getShards());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterName(), getType(), getUnitStrategyId(), getUnitStrategyName(), getShardStrategies(), getIdGenerators(), getDbCategory(), isEnabled(), isReleased(), getReleaseVersion(), getShards());
    }

    @Override
    public String toString() {
        return "DalCluster{" +
                "clusterName='" + clusterName + '\'' +
                ", type='" + type + '\'' +
                ", unitStrategyId=" + unitStrategyId +
                ", unitStrategyName='" + unitStrategyName + '\'' +
                ", shardStrategies='" + shardStrategies + '\'' +
                ", idGenerators='" + idGenerators + '\'' +
                ", dbCategory='" + dbCategory + '\'' +
                ", enabled=" + enabled +
                ", released=" + released +
                ", releaseVersion=" + releaseVersion +
                ", shards='" + shards + '\'' +
                '}';
    }
}
