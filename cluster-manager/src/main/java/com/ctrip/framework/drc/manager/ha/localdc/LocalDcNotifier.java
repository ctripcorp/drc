package com.ctrip.framework.drc.manager.ha.localdc;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.cluster.impl.InstanceStateController;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.NOTIFY_LOGGER;

/**
 * Created by jixinwang on 2022/11/11
 */
@Order(2)
@Component
public class LocalDcNotifier implements StateChangeHandler {

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private InstanceStateController instanceStateController;

    @Resource( name = AbstractSpringConfigContext.GLOBAL_EXECUTOR )
    private ExecutorService executors;

    @Override
    public void replicatorActiveElected(String clusterId, Replicator replicator) {
        // notify messenger in local dc
        Messenger activeMessenger = currentMetaManager.getActiveMessenger(clusterId);
        if (activeMessenger == null) {
            NOTIFY_LOGGER.info("[replicatorActiveElected][no active messenger, do nothing]{}", clusterId);
            return;
        }
        executors.execute(new LocalDcNotifyTask(clusterId, activeMessenger));
    }

    @Override
    public void messengerActiveElected(String clusterId, Messenger messenger) {

    }

    @Override
    public void applierMasterChanged(String clusterId, String backupClusterId, Pair<String, Integer> newMaster) {

    }

    @Override
    public void applierActiveElected(String clusterId, Applier applier) {

    }

    @Override
    public void mysqlMasterChanged(String clusterId, Endpoint master) {

    }

    public class LocalDcNotifyTask extends AbstractExceptionLogTask {

        private String clusterId;

        private Messenger activeMessenger;

        public LocalDcNotifyTask(String clusterId, Messenger activeMessenger) {
            this.clusterId = clusterId;
            this.activeMessenger = activeMessenger;
        }

        @Override
        protected void doRun() {
            NOTIFY_LOGGER.info("[replicatorActiveElected][notify local dc messenger]{}, {}", clusterId, activeMessenger);
            instanceStateController.addMessenger(clusterId, activeMessenger);
        }
    }

    public void setExecutors(ExecutorService executors) {
        this.executors = executors;
    }
}
