package com.ctrip.framework.drc.messenger.mq;

import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.KeyedEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.resource.position.MessengerGtidMergeTask;
import com.ctrip.framework.drc.fetcher.resource.position.MessengerGtidQueryTask;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.getDefaultPoolProperties;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.isIntegrityTest;

/**
 * Created by dengquanliang
 * 2025/1/9 15:44
 */
public class KafkaPositionResource extends AbstractResource implements MqPosition {
    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");
    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private static final int RETRY_TIME = 1;

    private static final int PERSIST_POSITION_PERIOD_TIME = 30;

    private DataSource dataSource;

    private Endpoint endpoint;

    @InstanceConfig(path = "target.ip")
    public String ip;

    @InstanceConfig(path = "target.port")
    public int port;

    @InstanceConfig(path = "target.username")
    public String username;

    @InstanceConfig(path = "target.password")
    public String password;

    @InstanceConfig(path = "gtidExecuted")
    public String initialGtidExecuted = "";

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    private GtidSet executedGtidSet = new GtidSet(StringUtils.EMPTY);

    private ExecutorService gtidService = ThreadUtils.newSingleThreadExecutor("MQ-Update-Position");

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("MQ-Persist-Position-Task");

    @Override
    protected void doInitialize() throws Exception {
        loggerMsg.info("[MQ][{}] persist mq position when mq position resource initialize", registryKey);

        endpoint = new KeyedEndPoint(registryKey, new DefaultEndPoint(ip, port, username, password));
        PoolProperties poolProperties = getDefaultPoolProperties(endpoint);
        dataSource = DataSourceManager.getInstance().getDataSource(endpoint, poolProperties);

        executedGtidSet = new GtidSet(get()).union(new GtidSet(initialGtidExecuted));
        startUpdatePositionSchedule();
    }

    @Override
    public void add(Gtid gtid) {
        gtidService.submit(() -> {
            executedGtidSet.add(gtid);
        });
    }

    @Override
    public void union(GtidSet gtidSet) {
        gtidService.submit(() -> {
            executedGtidSet = executedGtidSet.union(gtidSet);
        });
    }

    @Override
    public String get() {
        if (isIntegrityTest()) {
            return StringUtils.EMPTY;
        }
        GtidSet gtidSetFromDb= new GtidSet(getPositionFromDb());

        logger.info("[{}][NETWORK GTID] from db: {}", registryKey, gtidSetFromDb);
        return gtidSetFromDb.toString();
    }

    @Override
    public void release() throws Exception {
        dispose();
    }

    private void startUpdatePositionSchedule() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                persistPosition();
                loggerMsg.info("[MQ][{}] persist position schedule success", registryKey);
            } catch (Throwable t) {
                loggerMsg.error("[MQ][{}] persist position schedule error", registryKey, t);
            }
        }, PERSIST_POSITION_PERIOD_TIME, PERSIST_POSITION_PERIOD_TIME, TimeUnit.SECONDS);
    }

    protected void persistPosition() {
        if (isIntegrityTest()) {
            return;
        }
        updatePositionInDb(true);
    }

    private void updatePositionInDb(boolean needRetry) {
        String currentPosition = getCurrentPosition();
        if (needRetry) {
            Boolean res = new RetryTask<>(new MessengerGtidMergeTask(currentPosition, dataSource, registryKey), RETRY_TIME).call();
            if (res == null) {
                loggerTT.error("[TT][{}] merge gtid set error, shutdown server", registryKey);
                logger.info("transaction table status is stopped for {}", registryKey);
                getSystem().setStatus(SystemStatus.STOPPED);
            }
        } else {
            new RetryTask<>(new MessengerGtidMergeTask(currentPosition, dataSource, registryKey), 0).call();
        }
    }

    @VisibleForTesting
    protected String getCurrentPosition() {
        return executedGtidSet.toString();
    }

    private String getPositionFromDb() {
        MessengerGtidQueryTask gtidQueryTask = new MessengerGtidQueryTask(endpoint, registryKey);
        String gtidExecuted = gtidQueryTask.doQuery();
        return gtidExecuted;
    }

    @Override
    protected void doDispose() throws Exception {
        try {
            if (dataSource != null) {
                loggerMsg.info("[MQ][{}] persist mq position to db when dispose start", registryKey);
                updatePositionInDb(false);
                loggerMsg.info("[MQ][{}] persist mq position to db when dispose end", registryKey);
                DataSourceManager.getInstance().clearDataSource(endpoint);
            }
        } finally {
            if (gtidService != null) {
                gtidService.shutdown();
                gtidService = null;
            }
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
                scheduledExecutorService = null;
            }
        }
    }

}
