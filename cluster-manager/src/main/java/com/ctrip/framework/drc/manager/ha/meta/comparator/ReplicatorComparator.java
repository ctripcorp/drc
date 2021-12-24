package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.xpipe.tuple.Pair;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class ReplicatorComparator extends AbstractMetaComparator<Replicator, ReplicatorChange> {

    @SuppressWarnings("unused")
    private DbCluster current, future;

    public ReplicatorComparator(DbCluster current, DbCluster future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        List<Replicator> currentAll =  current.getReplicators();
        List<Replicator> futureAll =  future.getReplicators();


        Pair<List<Replicator>, List<Pair<Replicator, Replicator>>> subResult = sub(futureAll, currentAll);
        List<Replicator> tAdded = subResult.getKey();
        added.addAll(tAdded);

        List<Pair<Replicator, Replicator>> modified = subResult.getValue();
        compareConfigConfig(modified);

        List<Replicator> tRemoved = sub(currentAll, futureAll).getKey();
        removed.addAll(tRemoved);
    }


    private Pair<List<Replicator>, List<Pair<Replicator, Replicator>>> sub(List<Replicator> all1, List<Replicator> all2) {

        List<Replicator> subResult = new LinkedList<>();
        List<Pair<Replicator, Replicator>> intersectResult = new LinkedList<>();

        for(Replicator replicator1 : all1){

            Replicator replicator2Equal = null;
            for(Replicator replicator2 : all2){
                if(replicator1.equalsWithIpPort(replicator2)){
                    replicator2Equal = replicator2;
                    break;
                }
            }
            if(replicator2Equal == null){
                subResult.add(replicator1);
            }else{
                intersectResult.add(new Pair<>(replicator1, replicator2Equal));
            }
        }
        return new Pair<List<Replicator>, List<Pair<Replicator, Replicator>>>(subResult, intersectResult);
    }

    private void compareConfigConfig(List<Pair<Replicator, Replicator>> allModified) {

        for(Pair<Replicator, Replicator> pair : allModified){
            Replicator current = pair.getValue();
            Replicator future = pair.getKey();
            if(current.equals(future)){
                continue;
            }
            InstanceComparator instanceComparator = new InstanceComparator(current, future);
            instanceComparator.compare();
            modified.add(instanceComparator);
        }

    }

    @Override
    public String idDesc() {
        return current.getId();

    }

    public DbCluster getCurrent() {
        return current;
    }

    public DbCluster getFuture() {
        return future;
    }

}

