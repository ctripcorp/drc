package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.xpipe.tuple.Pair;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class DbComparator extends AbstractMetaComparator<Db, DbChange> {

    @SuppressWarnings("unused")
    private DbCluster current, future;

    public DbComparator(DbCluster current, DbCluster future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        List<Db> currentAll =  current.getDbs().getDbs();
        List<Db> futureAll =  future.getDbs().getDbs();


        Pair<List<Db>, List<Pair<Db, Db>>> subResult = sub(futureAll, currentAll);
        List<Db> tAdded = subResult.getKey();
        added.addAll(tAdded);

        List<Pair<Db, Db>> modified = subResult.getValue();
        compareConfigConfig(modified);

        List<Db> tRemoved = sub(currentAll, futureAll).getKey();
        removed.addAll(tRemoved);
    }


    private Pair<List<Db>, List<Pair<Db, Db>>> sub(List<Db> all1, List<Db> all2) {

        List<Db> subResult = new LinkedList<>();
        List<Pair<Db, Db>> intersectResult = new LinkedList<>();

        for(Db Db1 : all1){

            Db Db2Equal = null;
            for(Db Db2 : all2){
                if(Db1.equalsWithIpPort(Db2)){
                    Db2Equal = Db2;
                    break;
                }
            }
            if(Db2Equal == null){
                subResult.add(Db1);
            }else{
                intersectResult.add(new Pair<>(Db1, Db2Equal));
            }
        }
        return new Pair<List<Db>, List<Pair<Db, Db>>>(subResult, intersectResult);
    }

    private void compareConfigConfig(List<Pair<Db, Db>> allModified) {

        for(Pair<Db, Db> pair : allModified){
            Db current = pair.getValue();
            Db future = pair.getKey();
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

