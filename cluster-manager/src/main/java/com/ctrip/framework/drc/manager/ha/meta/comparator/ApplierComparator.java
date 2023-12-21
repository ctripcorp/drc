package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class ApplierComparator extends AbstractMetaComparator<Applier, ApplierChange> {

    @SuppressWarnings("unused")
    private DbCluster current, future;

    public ApplierComparator(DbCluster current, DbCluster future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        List<Applier> currentAll =  current.getAppliers();
        List<Applier> futureAll =  future.getAppliers();


        Pair<List<Applier>, List<Pair<Applier, Applier>>> subResult = sub(futureAll, currentAll);
        List<Applier> tAdded = subResult.getKey();
        added.addAll(tAdded);

        List<Pair<Applier, Applier>> modified = subResult.getValue();
        compareConfigConfig(modified);

        List<Applier> tRemoved = sub(currentAll, futureAll).getKey();
        removed.addAll(tRemoved);
    }


    private Pair<List<Applier>, List<Pair<Applier, Applier>>> sub(List<Applier> all1, List<Applier> all2) {

        List<Applier> subResult = new LinkedList<>();
        List<Pair<Applier, Applier>> intersectResult = new LinkedList<>();

        for(Applier applier1 : all1){

            Applier applier2Equal = null;
            for(Applier applier2 : all2){
                if(applier1.equalsWithIpPort(applier2)
                        && ObjectUtils.equals(applier1.getTargetMhaName(), applier2.getTargetMhaName())
                        && ObjectUtils.equals(applier1.getIncludedDbs(), applier2.getIncludedDbs())){
                    applier2Equal = applier2;
                    break;
                }
            }
            if(applier2Equal == null){
                subResult.add(applier1);
            }else{
                intersectResult.add(new Pair<>(applier1, applier2Equal));
            }
        }
        return new Pair<List<Applier>, List<Pair<Applier, Applier>>>(subResult, intersectResult);
    }

    private void compareConfigConfig(List<Pair<Applier, Applier>> allModified) {

        for(Pair<Applier, Applier> pair : allModified){
            Applier current = pair.getValue();
            Applier future = pair.getKey();
            if(current.equals(future)){
                continue;
            }

            ApplierPropertyComparator applierPropertyComparator = new ApplierPropertyComparator(current, future);
            applierPropertyComparator.compare();
            modified.add(applierPropertyComparator);
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

