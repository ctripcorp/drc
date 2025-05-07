package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jixinwang on 2022/11/1
 */
public class MessengerComparator extends AbstractMetaComparator<Messenger, MessengerChange> {

    @SuppressWarnings("unused")
    private DbCluster current, future;

    public MessengerComparator(DbCluster current, DbCluster future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        List<Messenger> currentAll = current.getMessengers();
        List<Messenger> futureAll = future.getMessengers();


        Pair<List<Messenger>, List<Pair<Messenger, Messenger>>> subResult = sub(futureAll, currentAll);
        List<Messenger> tAdded = subResult.getKey();
        added.addAll(tAdded);

        List<Pair<Messenger, Messenger>> modified = subResult.getValue();
        compareConfigConfig(modified);

        List<Messenger> tRemoved = sub(currentAll, futureAll).getKey();
        removed.addAll(tRemoved);
    }


    private Pair<List<Messenger>, List<Pair<Messenger, Messenger>>> sub(List<Messenger> all1, List<Messenger> all2) {

        List<Messenger> subResult = new LinkedList<>();
        List<Pair<Messenger, Messenger>> intersectResult = new LinkedList<>();

        for (Messenger messenger1 : all1) {

            Messenger messenger2Equal = null;
            for (Messenger messenger2 : all2) {
                if (messenger1.equalsWithIpPort(messenger2)
                        && ObjectUtils.equals(messenger1.getIncludedDbs(), messenger2.getIncludedDbs())
                        && ObjectUtils.equals(messenger1.getApplyMode(), messenger2.getApplyMode())) {
                    messenger2Equal = messenger2;
                    break;
                }
            }
            if (messenger2Equal == null) {
                subResult.add(messenger1);
            } else {
                intersectResult.add(new Pair<>(messenger1, messenger2Equal));
            }
        }
        return new Pair<List<Messenger>, List<Pair<Messenger, Messenger>>>(subResult, intersectResult);
    }

    private void compareConfigConfig(List<Pair<Messenger, Messenger>> allModified) {

        for (Pair<Messenger, Messenger> pair : allModified) {
            Messenger current = pair.getValue();
            Messenger future = pair.getKey();
            if (current.equals(future)) {
                continue;
            }
            MessengerPropertyComparator messengerPropertyComparator = new MessengerPropertyComparator(current, future);
            messengerPropertyComparator.compare();
            modified.add(messengerPropertyComparator);
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
