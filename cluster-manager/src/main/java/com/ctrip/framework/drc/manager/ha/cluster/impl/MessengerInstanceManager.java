package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparatorVisitor;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.MessengerComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by jixinwang on 2022/11/1
 */
@Component
public class MessengerInstanceManager extends AbstractInstanceManager implements TopElement {

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();

        MessengerComparator messengerComparator = comparator.getMessengerComparator();
        messengerComparator.accept(new MessengerInstanceManager.MessengerComparatorVisitor(clusterId));
    }

    @Override
    protected void handleClusterDeleted(DbCluster dbCluster) {
        String clusterId = dbCluster.getId();
        for (Messenger messenger : dbCluster.getMessengers()) {
            removeMessenger(clusterId, messenger);
        }
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {

        String clusterId = dbCluster.getId();
        List<Messenger> messengers = dbCluster.getMessengers();
        for (Messenger messenger : messengers) {
            registerMessenger(clusterId, messenger);
        }
    }

    private void registerMessenger(String clusterId, Messenger messenger) {
        try {
            instanceStateController.registerMessenger(clusterId, messenger);
        } catch (Exception e) {
            logger.error(String.format("[registerMessenger]%s,%s", clusterId, messenger), e);
        }
    }

    private void removeMessenger(String clusterId, Messenger messenger) {
        try {
            instanceStateController.removeMessenger(clusterId, messenger);
        } catch (Exception e) {
            logger.error(String.format("[addMessenger]%s,%s", clusterId, messenger), e);
        }
    }

    public class MessengerComparatorVisitor implements MetaComparatorVisitor<Messenger> {

        private String clusterId;

        public MessengerComparatorVisitor(String clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public void visitAdded(Messenger added) {
            logger.info("[visitAdded][add Messenger]{}", added);
            registerMessenger(clusterId, added);
        }

        @Override
        public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {

        }

        @Override
        public void visitRemoved(Messenger removed) {
            logger.info("[visitRemoved][remove Messenger]{}", removed);
            removeMessenger(clusterId, removed);

        }
    }
}
