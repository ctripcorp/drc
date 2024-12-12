package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.impl.MetaRefreshDone;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author yongnian
 * @create 2024/11/1 14:23
 */
@Component
public class ClusterServerStateManager extends AbstractLifecycleObservable implements Observer {
    @Autowired
    private ZkClient zkClient;
    @Autowired
    private ClusterManagerConfig config;
    @Autowired
    private RegionCache regionCache;

    private final ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getClass().getSimpleName()));
    private final ServerState state = new ServerState(ServerStateEnum.NORMAL);

    @Override
    protected void doInitialize() throws Exception {
        regionCache.addObserver(this);
    }

    @Override
    protected void doStart() throws Exception {
        this.registerConnectionStateListener();
        scheduled.scheduleWithFixedDelay(this::logState, 60, 60, TimeUnit.SECONDS);
    }


    private void logState() {
        logger.info("[{}] Current state:{}", config.getClusterServerIp(), state.serverStateEnum);
        EventMonitor.DEFAULT.logEvent("drc.cm.server.state", state.serverStateEnum.toString());
    }

    public ServerStateEnum getServerState() {
        return state.serverStateEnum;
    }

    private void registerConnectionStateListener() {
        CuratorFramework curatorFramework = zkClient.get();
        curatorFramework.getConnectionStateListenable().addListener(this::stateChanged);
    }


    @VisibleForTesting
    protected void stateChanged(CuratorFramework client, ConnectionState newState) {
        logger.warn("[zkConnectionStateChange][{}] State:{}", config.getClusterServerIp(), newState);
        switch (newState) {
            case LOST:
                this.serverDead();
                break;
            case RECONNECTED:
                this.serverReconnect();
                break;
            default:
                break;
        }
    }

    private synchronized void serverReconnect() {
        if (state.pushTo(ServerStateEnum.RESTARTING)) {
            // only after all meta info updated, can this cm server return to normal
            regionCache.triggerRefreshAll();
            notifyObservers(state.serverStateEnum);
        }
    }

    private synchronized void serverDead() {
        if (state.pushTo(ServerStateEnum.LOST)) {
            notifyObservers(state.serverStateEnum);
        }

    }

    private synchronized void serverAlive() {
        if (state.pushTo(ServerStateEnum.NORMAL)) {
            notifyObservers(state.serverStateEnum);
        }
    }

    // after meta info updated, this cm server can return to normal
    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof MetaRefreshDone) {
            this.serverAlive();
        }
    }


    static class ServerState {
        private ServerStateEnum serverStateEnum;

        public ServerState(ServerStateEnum serverStateEnum) {
            this.serverStateEnum = serverStateEnum;
        }

        public synchronized boolean pushTo(ServerStateEnum stateEnum) {
            if (serverStateEnum.pushTo(stateEnum)) {
                serverStateEnum = stateEnum;
                return true;
            }
            return false;
        }
    }

}
