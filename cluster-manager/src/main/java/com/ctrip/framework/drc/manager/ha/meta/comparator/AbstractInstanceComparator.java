package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public abstract class AbstractInstanceComparator extends AbstractMetaComparator<Instance, InstanceChange> {

    @SuppressWarnings("unused")
    private DbCluster current, future;

    public AbstractInstanceComparator(DbCluster current, DbCluster future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        List<Instance> currentAll =  getInstance(current);
        List<Instance> futureAll =  getInstance(future);


        Pair<List<Instance>, List<Pair<Instance, Instance>>> subResult = sub(futureAll, currentAll);
        List<Instance> tAdded = subResult.getKey();
        added.addAll(tAdded);

        List<Pair<Instance, Instance>> modified = subResult.getValue();
        compareConfigConfig(modified);

        List<Instance> tRemoved = sub(currentAll, futureAll).getKey();
        removed.addAll(tRemoved);
    }


    private Pair<List<Instance>, List<Pair<Instance, Instance>>> sub(List<Instance> all1, List<Instance> all2) {

        List<Instance> subResult = new LinkedList<>();
        List<Pair<Instance, Instance>> intersectResult = new LinkedList<>();

        for(Instance replicator1 : all1){

            Instance replicator2Equal = null;
            for(Instance replicator2 : all2){
                if(theSame(replicator1, replicator2)){
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
        return new Pair<List<Instance>, List<Pair<Instance, Instance>>>(subResult, intersectResult);
    }

    private void compareConfigConfig(List<Pair<Instance, Instance>> allModified) {

        for(Pair<Instance, Instance> pair : allModified){
            Instance current = pair.getValue();
            Instance future = pair.getKey();
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
        return null;

    }

    public DbCluster getCurrent() {
        return current;
    }

    public DbCluster getFuture() {
        return future;
    }


    public static boolean theSame(Instance replicator1, Instance replicator2) {

        if(replicator1== null){
            return replicator2 == null;
        }else if(replicator2 == null){
            return false;
        }

        if(!ObjectUtils.equals(replicator1.getIp(), replicator2.getIp())){
            return false;
        }

        if(!ObjectUtils.equals(replicator1.getPort(), replicator2.getPort())){
            return false;
        }

        return true;
    }

    protected abstract List<Instance> getInstance(DbCluster dbCluster);
}

