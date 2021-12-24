package com.ctrip.framework.drc.core.meta;

import java.util.Objects;

/**
 * @Author Slight
 * Nov 07, 2019
 */
public class ApplierMeta extends InstanceInfo {

    public InstanceInfo replicator;
    public DBInfo target;

    public InstanceInfo getReplicator() {
        return replicator;
    }

    public void setReplicator(InstanceInfo replicator) {
        this.replicator = replicator;
    }

    public DBInfo getTarget() {
        return target;
    }

    public void setTarget(DBInfo target) {
        this.target = target;
    }

    @Override
    public String toString() {
        return "ApplierMeta{" +
                "replicator=" + replicator +
                ", target=" + target +
                ", name='" + name + '\'' +
                ", port=" + port +
                ", ip='" + ip + '\'' +
                ", idc='" + idc + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApplierMeta)) return false;
        if (!super.equals(o)) return false;
        ApplierMeta that = (ApplierMeta) o;
        return Objects.equals(replicator, that.replicator) &&
                Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), replicator, target);
    }
}
