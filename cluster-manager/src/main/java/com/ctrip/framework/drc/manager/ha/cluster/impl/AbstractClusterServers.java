package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.*;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class AbstractClusterServers<T extends ClusterServer> extends AbstractLifecycleObservable implements ClusterServers<T>, TopElement {

    private Map<String, T> servers = new ConcurrentHashMap<>();

    @Autowired
    private ClusterManagerConfig config;

    @Autowired
    private ZkClient zkClient;

    @Autowired
    private T currentServer;

    private PathChildrenCache serversCache;

    @Autowired
    private RemoteClusterServerFactory<T> remoteClusterServerFactory;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getClass().getSimpleName()));

    private ScheduledFuture<?> future;

    private volatile long lastChildEventTime = System.currentTimeMillis();

    @Override
    protected void doInitialize() throws Exception {

    }

    @Override
    protected void doStart() throws Exception {

        CuratorFramework client = zkClient.get();

        serversCache = new PathChildrenCache(client, ClusterZkConfig.getClusterManagerRegisterPath(), true,
                XpipeThreadFactory.create(String.format("PathChildrenCache(%s)", currentServer.getServerId())));
        serversCache.getListenable().addListener(new ChildrenChanged());
        serversCache.start();

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            public void doRun() {
                try {
                    childrenChanged();
                } catch (Throwable th) {
                    logger.error("[doStart]", th);
                }

            }
        }, 1000, config.getClusterServersRefreshMilli(), TimeUnit.MILLISECONDS);

    }

    protected class ChildrenChanged implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

            logger.info("[childEvent]{}, {}", event.getType(), ZkUtils.toString(event.getData()));
            childrenChanged();
        }
    }

    @Override
    public T currentClusterServer() {

        return currentServer;
    }

    @Override
    public T getClusterServer(String serverId) {
        return servers.get(serverId);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    //for test
    public String getServerIdFromPath(String path, String serverBasePath) {

        int index = path.indexOf(serverBasePath);
        if (index >= 0) {
            path = path.substring(index + serverBasePath.length());
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        return path;
    }

    //path: IP:PORT easy for test
    private synchronized void childrenChanged() throws ClusterException {
        lastChildEventTime = System.currentTimeMillis();
        try {
            logger.info("[childrenChanged][start][{}]{}", currentServerId(), servers);
            List<ChildData> allServers = serversCache.getCurrentData();
            String serverBasePath = ClusterZkConfig.getClusterManagerRegisterPath();

            Set<String> currentServers = new HashSet<>();
            for (ChildData childData : allServers) {

                String serverIdStr = getServerIdFromPath(childData.getPath(), serverBasePath);
                byte[] data = childData.getData();
                ClusterServerInfo info = Codec.DEFAULT.decode(data, ClusterServerInfo.class);
                if (info.getStateEnum().notAlive()) {
                    // To avoid deprecated cached data causing problem after server's reconnecting to zk
                    // make sure to notify **only** after meta info is refreshed (i.e. server is really alive)
                    logger.warn("[childrenChanged][{}][skipDead]{}{}", currentServerId(), serverIdStr, info);
                    continue;
                }

                logger.debug("[childrenChanged][{}]{},{}", currentServerId(), serverIdStr, info);
                currentServers.add(serverIdStr);

                ClusterServer server = servers.get(serverIdStr);
                if (server == null) {
                    logger.info("[childrenChanged][{}][createNew]{}{}", currentServerId(), serverIdStr, info);
                    T remoteServer = remoteClusterServerFactory.createClusterServer(serverIdStr, info);
                    servers.put(serverIdStr, remoteServer);
                    logger.info("[childrenChanged][{}][createNew]{}", currentServerId(), servers);
                    serverAdded(remoteServer);
                } else {
                    if (!info.equals(server.getClusterInfo())) {
                        logger.info("[childrenChanged][{}][clusterInfoChanged]{}{}", currentServerId(), serverIdStr, info, server.getClusterInfo());
                        T newServer = remoteClusterServerFactory.createClusterServer(serverIdStr, info);
                        servers.put(serverIdStr, newServer);
                        serverChanged(server, newServer);
                    }
                }
            }

            for (String old : servers.keySet()) {
                if (!currentServers.contains(old)) {
                    ClusterServer serverInfo = servers.remove(old);
                    logger.info("[childrenChanged][remote not exist][{}]{}, {}, current:{}", currentServerId(), old, serverInfo);
                    remoteDelted(serverInfo);

                }
            }
            logger.info("[childrenChanged][ end ][{}]{}", currentServerId(), servers);
        } catch (Exception e) {
            throw new ClusterException("[childrenChanged]", e);
        } finally {
            lastChildEventTime = System.currentTimeMillis();
        }
    }

    public long getLastChildEventTime() {
        return lastChildEventTime;
    }

    private Object currentServerId() {
        return currentServer.getServerId();
    }

    private void remoteDelted(ClusterServer serverInfo) {
        notifyObservers(new NodeDeleted<ClusterServer>(serverInfo));
    }

    private void serverChanged(ClusterServer oldServer, ClusterServer newServer) {
        notifyObservers(new NodeModified<ClusterServer>(oldServer, newServer));
    }

    private void serverAdded(ClusterServer remoteServer) {
        notifyObservers(new NodeAdded<ClusterServer>(remoteServer));
    }

    public void setconfig(ClusterManagerConfig config) {
        this.config = config;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void setCurrentServer(T currentServer) {
        this.currentServer = currentServer;
    }

    public void setRemoteClusterServerFactory(RemoteClusterServerFactory<T> remoteClusterServerFactory) {
        this.remoteClusterServerFactory = remoteClusterServerFactory;
    }

    @Override
    public Set<T> allClusterServers() {
        return new HashSet<>(servers.values());
    }


    @Override
    protected void doStop() throws Exception {

        serversCache.close();
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    @Override
    public void refresh() throws ClusterException {
        childrenChanged();
    }

    @Override
    public boolean exist(String serverId) {
        return servers.get(serverId) != null;
    }

    @Override
    public Map<String, ClusterServerInfo> allClusterServerInfos() {

        Map<String, ClusterServerInfo> result = new HashMap<>();
        for (Map.Entry<String , T> entry : servers.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getClusterInfo());
        }
        return result;
    }


}
