package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.exception.dump.BinlogDumpGtidException;
import com.ctrip.framework.drc.core.exception.dump.EventConvertException;
import com.ctrip.framework.drc.core.monitor.log.Accumulation;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.activity.replicator.config.FetcherSlaveConfig;
import com.ctrip.framework.drc.fetcher.event.MonitoredGtidLogEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.Capacity;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import com.ctrip.framework.drc.fetcher.system.*;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.COMMA;

/**
 * @Author Slight
 * Sep 29, 2019
 */
public abstract class DumpEventActivity<T> extends AbstractActivity implements TaskSource<T>, AcquireActivity {

    protected final Logger loggerER = LoggerFactory.getLogger("EVT RECV");
    protected final Accumulation accumulationGW = new Accumulation("GAQ WAIT", "ms");

    protected FetcherSlaveConfig config;
    protected FetcherSlaveServer server;
    protected NetworkContextResource context;

    public TaskActivity<T, ?> latter;

    @InstanceResource
    public Capacity capacity;

    @InstanceResource
    public ListenableDirectMemory listenableDirectMemory;

    protected LogEventHandler eventHandler;

    @Override
    public void doInitialize() throws Exception {
        eventHandler = getLogEventHandler();
        context = getNetworkContextResource();
        context.initialize();
    }

    @InstanceConfig(path = "replicator.ip")
    public String replicatorIp;

    @InstanceConfig(path = "replicator.port")
    public int replicatorPort;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    @InstanceConfig(path = "includedDbs")
    public String includedDbs;

    @InstanceConfig(path = "nameFilter")
    public String nameFilter;

    @InstanceConfig(path = "routeInfo")
    public String routeInfo;

    @InstanceConfig(path = "applyMode")
    public int applyMode = ApplyMode.set_gtid.getType();

    @InstanceConfig(path = "properties")
    public String properties;

    @Override
    public void doStart() throws Exception {
        if (StringUtils.isNotBlank(routeInfo)) {
            ProxyRegistry.registerProxy(replicatorIp, replicatorPort, routeInfo);
        }
        Endpoint endpoint = new DefaultEndPoint(replicatorIp, replicatorPort);
        config = new FetcherSlaveConfig();
        config.setEndpoint(endpoint);
        config.setRegistryKey(registryKey);
        config.setGtidSet(context.fetchGtidSet());
        config.setApplierName(registryKey);
        config.setApplyMode(applyMode);
        if (StringUtils.isNotBlank(properties)) {
            config.setProperties(properties);
        }
        if (StringUtils.isNotBlank(includedDbs)) {
            config.setIncludedDbs(Sets.newHashSet(StringUtils.split(includedDbs, COMMA)));
        }

        if (StringUtils.isNotBlank(nameFilter)) {
            config.setNameFilter(nameFilter);
        }

        logger.info("[CONNECT to replicator] {}", config);
        server = getFetcherSlaveServer();
        server.setLogEventHandler(eventHandler);
        server.setNetworkContextResource(context);
        server.initialize();
        server.start();
    }

    @Override
    public void doStop() throws Exception {
        if (StringUtils.isNotBlank(routeInfo)) {
            ProxyRegistry.unregisterProxy(replicatorIp, replicatorPort);
        }
        server.stop();
        server.dispose();
        server = null;
    }

    @Override
    public <U> TaskActivity<T, U> link(TaskActivity<T, U> latter) {
        this.latter = latter;
        return latter;
    }

    protected boolean exceptionCheck(BinlogDumpGtidException exception) {
        if (exception != null) {
            if (exception instanceof EventConvertException) {
                logger.error("convert error when dumping event, going to stop & dispose applier server(instance): ", exception);
                try {
                    system.stop();
                    system.dispose();
                } catch (Exception e) {
                    logger.error("fail to stop & dispose applier server(instance) - UNLIKELY");
                }
                return false;
            } else {
                logger.error("UNLIKELY - unknown exception", exception);
                return false;
            }
        }

        return true;
    }

    protected boolean beforeHandleLogEvent(LogEvent logEvent, LogEventCallBack logEventCallBack, BinlogDumpGtidException exception) {
        return exceptionCheck(exception) && rateLimit(logEvent, logEventCallBack) && !heartBeat(logEvent, logEventCallBack);
    }

    protected boolean heartBeat(LogEvent logEvent, LogEventCallBack logEventCallBack){
        return false;
    }

    protected boolean rateLimit(LogEvent logEvent, LogEventCallBack logEventCallBack) {
        memoryCheck(logEvent, logEventCallBack);
        if (logEvent instanceof MonitoredGtidLogEvent) {
            MonitoredGtidLogEvent event = ((MonitoredGtidLogEvent) logEvent);
            try {
                while (!tryAcquire(100, TimeUnit.MILLISECONDS)) {
                    if (isDisposed()) {
                        event.release();
                        logger.info("DumpEventActivity: QUIT");
                        return false;
                    }
                }
            } catch (InterruptedException e) {
                event.release();
                return false;
            }
        }

        return true;
    }

    private void memoryCheck(LogEvent logEvent, LogEventCallBack logEventCallBack) {
        if (logEvent instanceof DirectMemoryAware) {
            DirectMemoryAware directMemoryAware = (DirectMemoryAware) logEvent;
            directMemoryAware.setDirectMemory(listenableDirectMemory);
            if(!listenableDirectMemory.tryAllocate(logEvent.getLogEventHeader().getEventSize())) {
                logEventCallBack.onFailure();
                listenableDirectMemory.addListener(logEventCallBack);
            }
        }
    }

    protected void afterHandleLogEvent(T logEvent) {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("instance", registryKey, 1, 0);
        loggerER.info("{} {} - RECEIVED - {}", registryKey, context.fetchGtid(), logEvent.getClass().getSimpleName());
        try {
            long start = System.currentTimeMillis();
            latter.waitSubmit(logEvent);
            accumulationGW.add(System.currentTimeMillis() - start);
        } catch (InterruptedException e) {
            logger.error("GAQ.put() - PROBABLY when dump event activity is disposing.", e);
        }
    }

    protected abstract FetcherSlaveServer getFetcherSlaveServer();

    protected LogEventHandler getLogEventHandler() {
        return (logEvent, logEventCallBack, exception) -> {
            if (beforeHandleLogEvent(logEvent, logEventCallBack, exception)) {
                doHandleLogEvent((T) logEvent);
                afterHandleLogEvent((T) logEvent);
            }
        };
    }

    protected void doHandleLogEvent(T logEvent) {}

    protected NetworkContextResource getNetworkContextResource() throws Exception {
        return (NetworkContextResource) derive (NetworkContextResource.class);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return capacity.tryAcquire(100, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void setContext(NetworkContextResource context) {
        this.context = context;
    }
}
