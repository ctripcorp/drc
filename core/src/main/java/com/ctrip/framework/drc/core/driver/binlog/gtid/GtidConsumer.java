package com.ctrip.framework.drc.core.driver.binlog.gtid;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @Author limingdong
 * @create 2019/12/31
 */
public class GtidConsumer {

    private static final Logger logger = LoggerFactory.getLogger(GtidConsumer.class);

    private GtidSet gtidSet = new GtidSet(new LinkedHashMap<>());

    private ExecutorService gtidService = Executors.newCachedThreadPool(new NamedThreadFactory("Gtid-Consume"));

    private Set<String> gtidSetString = Sets.newHashSet();

    private BlockingQueue<String> gtidQueue = new LinkedBlockingDeque<String>();

    private String lastGtidInQueue = StringUtils.EMPTY;

    private Future future;

    public GtidConsumer(boolean executed) {
        if (executed) {
            startConsume();
        }
    }

    public boolean add(String gtid) {
        return gtidSetString.add(gtid);
    }

    public boolean offer(String gtid) {
        return gtidQueue.offer(gtid);
    }

    public void init(GtidSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    public GtidSet getGtidSet() {
        while (!gtidQueue.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
        }
        int size = gtidSetString.size();
        if (size > 0) {
            logger.info("[gtidSetString] size is {}", size);
            for (String gtid : gtidSetString) {
                gtidSet.add(gtid);
            }
            gtidSetString.clear();
        }
        logger.info("[Consume] gtid queue completely and return executed gtid {}", gtidSet);
        gtidSet.add(lastGtidInQueue);
        future.cancel(true);
        return gtidSet;
    }

    private void startConsume() {
        future = gtidService.submit(() -> {
            try {
                while (true) {
                    try {
                        lastGtidInQueue = gtidQueue.take();
                    } catch (InterruptedException e) {
                        break;
                    }
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.gtid.restore", "executed", new Task() {
                        @Override
                        public void go() {
                            gtidSet.add(lastGtidInQueue);
                        }
                    });
                }
            } catch (Exception e) {
                logger.error("consume gtid error", e);
            }
            logger.info("gtidService finished and return");
        });
    }
}
