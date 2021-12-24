package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class ClusterComparator extends AbstractMetaComparator<Instance, DbClusterChange> {

    @SuppressWarnings("unused")
    private DbCluster current, future;

    private ReplicatorComparator replicatorComparator;

    private ApplierComparator applierComparator;

    private DbComparator dbComparator;

    public ClusterComparator(DbCluster current, DbCluster future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        replicatorComparator = new ReplicatorComparator(current, future);
        replicatorComparator.compare();

        applierComparator = new ApplierComparator(current, future);
        applierComparator.compare();

        dbComparator = new DbComparator(current, future);
        dbComparator.compare();
    }

    @Override
    public String idDesc() {
        if(current != null){
            return current.getId();
        }
        if(future != null){
            return future.getId();
        }
        return null;

    }

    @Override
    public String toString() {
        return String.format("%s: replicator:{added:%s, removed:%s, changed:%s}, applier:{added:%s, removed:%s, changed:%s}, dbs:{added:%s, removed:%s, changed:%s}", idDesc(), replicatorComparator.getAdded(), replicatorComparator.getRemoved(), replicatorComparator.getMofified(),
                applierComparator.getAdded(), applierComparator.getRemoved(), applierComparator.getMofified(), dbComparator.getAdded(), dbComparator.getRemoved(), dbComparator.getMofified());
    }

    public DbCluster getCurrent() {
        return current;
    }

    public DbCluster getFuture() {
        return future;
    }

    public ReplicatorComparator getReplicatorComparator() {
        return replicatorComparator;
    }

    public ApplierComparator getApplierComparator() {
        return applierComparator;
    }

    public DbComparator getDbComparator() {
        return dbComparator;
    }
}

