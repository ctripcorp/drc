package com.ctrip.framework.drc.manager.healthcheck.tracker;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.healthcheck.HeartBeatContext;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Created by mingdongli
 * 2019/11/21 下午2:54.
 */
public class HeartBeatTrackerImpl extends AbstractLifecycle implements HeartBeatTracker, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HeartBeatTrackerImpl.class);

    private ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("HeartBeatTrackerImpl-Run");

    private Map<Endpoint, HeartbeatImpl> heartbeatsById = Maps.newConcurrentMap();  //key是heartbeatId=ip

    private Map<Long, HeartbeatSet> heartbeatSets = Maps.newConcurrentMap();  //key是currentTime + n * tickTime

    private HeartBeatContext heartBeatContext;

    private HeartbeatExpirer expirer;

    private long nextExpirationTime;

    private int expirationInterval;

    private volatile long currentTime;

    public HeartBeatTrackerImpl(HeartBeatContext heartBeatContext) {
        this.expirationInterval = heartBeatContext.getInterval();
        nextExpirationTime = roundToInterval(System.currentTimeMillis());
        this.heartBeatContext = heartBeatContext;
    }

    private long roundToInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    public void setExpirer(HeartbeatExpirer expirer) {
        this.expirer = expirer;
    }

    @Override
    public void createHeartbeat(Endpoint key, int heartbeatTimeout) {
        addHeartbeat(key, heartbeatTimeout);
    }

    @Override
    public synchronized void addHeartbeat(Endpoint id, int heartbeatTimeout) {
        if (heartbeatsById.get(id) == null) {
            HeartbeatImpl h = new HeartbeatImpl(id, heartbeatTimeout, 0);
            heartbeatsById.put(id, h);
        }
        touchHeartbeat(id, heartbeatTimeout);
    }

    @Override
    public synchronized boolean touchHeartbeat(Endpoint heartbeatId, int heartbeatTimeout) {
        HeartbeatImpl h = heartbeatsById.get(heartbeatId);
        if (h == null || h.isClosing()) {
            return false;
        }
        long expireTime = roundToInterval(System.currentTimeMillis() + heartbeatTimeout);  //超时桶
        if (h.tickTime >= expireTime) {  //不用往后移动了
            // Nothing needs to be done
            return true;
        }
        HeartbeatSet set = heartbeatSets.get(h.tickTime);
        if (set != null) {
            set.heartbeats.remove(h);  //移除旧的
        }
        h.tickTime = expireTime;
        set = heartbeatSets.get(h.tickTime);
        if (set == null) {
            set = new HeartbeatSet();
            heartbeatSets.put(expireTime, set);
        }
        set.heartbeats.add(h);  //插入到新的桶里
        return true;
    }

    @Override
    public boolean hasHeartbeat(Endpoint heartbeatId) {
        HeartbeatImpl h = heartbeatsById.get(heartbeatId);
        return h == null ? false : true;
    }

    @Override
    public void setHeartbeatClosing(Endpoint heartbeatId) {
        HeartbeatImpl h = heartbeatsById.get(heartbeatId);
        if (h == null) {
            return;
        }
        h.isClosing = true;
    }

    @Override
    public void removeHeartbeat(Endpoint heartbeatId) {
        HeartbeatImpl s = heartbeatsById.remove(heartbeatId);
        if (s != null) {
            HeartbeatSet set = heartbeatSets.get(s.tickTime);
            if (set != null) {
                logger.info("remove heartbeat of {}", heartbeatId);
                set.heartbeats.remove(s);
            }
        }
    }

    @Override
    protected void doStart() throws Exception {
        executorService.submit(this::run);
    }

    @Override
    protected void doStop() throws Exception {
        heartbeatsById.clear();
        heartbeatSets.clear();
    }

    @Override
    public synchronized void run() {
        while (getLifecycleState().isInitialized()) {
            try {
                currentTime = System.currentTimeMillis();
                if (nextExpirationTime > currentTime) {
                    this.wait(nextExpirationTime - currentTime);
                    continue;
                }
                HeartbeatSet set;
                set = heartbeatSets.remove(nextExpirationTime);  //不停的移除
                if (set != null) {
                    List<Endpoint> downSchedulers = new ArrayList<Endpoint>();
                    for (HeartbeatImpl h : set.heartbeats) {
                        setHeartbeatClosing(h.heartbeatId);
                        removeHeartbeat(h.heartbeatId);
                        if (heartBeatContext.accept(h.heartbeatId)) {
                            logger.info("down master add {}", h.heartbeatId);
                            downSchedulers.add(h.getHeartbeatId());
                        }
                    }
                    if (!downSchedulers.isEmpty()) {
                        expirer.expire(downSchedulers);
                    }
                }
                nextExpirationTime += expirationInterval;
            } catch (Throwable t) {
                nextExpirationTime += expirationInterval;
                logger.error("run error", t);
            }
        }
        logger.info("HeartBeatTrackerImpl exited loop!");
    }

    public static class HeartbeatImpl implements HeartBeatTracker.Heartbeat {

        final Endpoint heartbeatId;

        final int timeout;

        long tickTime;

        boolean isClosing;

        HeartbeatImpl(Endpoint heartbeatId, int timeout, long expireTime) {
            this.heartbeatId = heartbeatId;
            this.timeout = timeout;
            this.tickTime = expireTime;
            isClosing = false;
        }

        @Override
        public Endpoint getHeartbeatId() {
            return heartbeatId;
        }

        @Override
        public int getTimeout() {
            return timeout;
        }

        @Override
        public boolean isClosing() {
            return isClosing;
        }
    }

    static class HeartbeatSet {
        HashSet<HeartbeatImpl> heartbeats = new HashSet<HeartbeatImpl>();
    }
}
