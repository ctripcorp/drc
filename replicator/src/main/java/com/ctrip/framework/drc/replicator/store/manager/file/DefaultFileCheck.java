package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.GtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.ShowMasterGtidReader;
import com.ctrip.framework.drc.core.driver.command.netty.codec.FileCheck;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.channel.Channel;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * Created by jixinwang on 2023/8/15
 */
public class DefaultFileCheck implements FileCheck {

    private int CHECK_PERIOD = CONNECTION_IDLE_TIMEOUT_SECOND * 1000 * 2;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private FileManager fileManager;

    private String registerKey;

    private long lastLogSize;

    private Endpoint endpoint;

    private ScheduledExecutorService scheduledExecutor;

    public DefaultFileCheck(String registerKey, FileManager fileManager, Endpoint endpoint) {
        this.registerKey = registerKey;
        this.fileManager = fileManager;
        this.endpoint = endpoint;
    }

    @Override
    public void start(Channel channel) {
        logger.info("[file][check] for {} start, with channel {}", registerKey, channel.toString());
        if (scheduledExecutor == null) {
            scheduledExecutor = ThreadUtils.newSingleThreadScheduledExecutor("File-Check-" + registerKey);
        }

        lastLogSize = 0;
        long initialDelay = new Random().nextInt(CHECK_PERIOD);

        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                long currentLogSize = fileManager.getCurrentLogSize();
                if (filePositionMoving(currentLogSize)) {
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("DRC.file.check.true", registerKey, 1, 0);
                    HEARTBEAT_LOGGER.info("[file][check] for {} true, lastSize: {}, currentSize: {}, channel: {}", registerKey, lastLogSize, currentLogSize, channel.toString());
                    lastLogSize = currentLogSize;
                } else {
                    HEARTBEAT_LOGGER.info("[file][check] for {} false, lastSize: {}, currentSize: {}, channel: {}", registerKey, lastLogSize, currentLogSize, channel.toString());
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("DRC.file.check.false", registerKey, 1, 0);
                    boolean receiveCheckSwitch = DynamicConfig.getInstance().getReceiveCheckSwitch();
                    if (receiveCheckSwitch) {
                        logger.info("[file][check] for {} false, close channel {}", registerKey, channel.toString());
                        channel.close();
                    }
                }
            } catch (Throwable t) {
                logger.info("[file][check] for {} exception, with channel {}", registerKey, channel.toString(), t);
            }
        }, initialDelay, CHECK_PERIOD, TimeUnit.MILLISECONDS);
    }

    private boolean filePositionMoving(long currentLogSize) throws InterruptedException {
        if (lastLogSize != 0 && lastLogSize == currentLogSize) {
            return !gtidSetMoving();
        }
        return true;
    }

    @VisibleForTesting
    protected boolean gtidSetMoving() throws InterruptedException {
        List<GtidReader> gtidReaderList = Lists.newArrayList();
        gtidReaderList.add(new ShowMasterGtidReader());
        ExecutedGtidQueryTask queryTask = new ExecutedGtidQueryTask(endpoint, gtidReaderList);
        String firstResult = queryTask.call();
        logger.info("[file][check] for {}, first query gtidset: {}", registerKey, firstResult);
        Thread.sleep(5 * 1000);
        String secondResult = queryTask.call();
        logger.info("[file][check] for {}, second query gtidset: {}", registerKey, secondResult);
        return !Objects.equals(firstResult, secondResult);
    }

    @Override
    public void stop() {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;
        }
    }

    @VisibleForTesting
    protected void setCheckPeriod(int checkPeriod) {
        this.CHECK_PERIOD = checkPeriod;
    }
}
