package com.ctrip.framework.drc.replicator.store.manager.gtid;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import com.google.common.collect.Sets;
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

    private Set<String> gtidSetString = Sets.newHashSet();


    private ExecutorService gtidService = Executors.newCachedThreadPool(new NamedThreadFactory("Gtid-Consume"));

    private BlockingQueue<GtidLogEvent> gtidQueue = new LinkedBlockingDeque<GtidLogEvent>();

    private volatile boolean threadFinished = false;

    public GtidConsumer(boolean executed) {
        if (executed) {
            startConsume();
        }
    }

    public boolean offer(GtidLogEvent gtidLogEvent) {
        return gtidQueue.offer(gtidLogEvent);
    }

    public boolean offer(String gtid) {
        return gtidSetString.add(gtid);
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
        threadFinished = true;
        return gtidSet;
    }

    private void startConsume() {
        gtidService.submit(() -> {
            try {
                while (!threadFinished) {
                    GtidLogEvent gtidLogEvent = gtidQueue.poll(5, TimeUnit.SECONDS);
                    if (gtidLogEvent == null) {
                        continue;
                    }
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.gtid.restore", "executed", new Task() {
                        @Override
                        public void go() {
                            String gtidString = gtidLogEvent.getGtid();
                            gtidSet.add(gtidString);
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
