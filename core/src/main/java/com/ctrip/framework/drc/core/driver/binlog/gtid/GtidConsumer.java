package com.ctrip.framework.drc.core.driver.binlog.gtid;

import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
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

    private BlockingQueue<GtidLogEvent> gtidEventQueue = new LinkedBlockingDeque<GtidLogEvent>();

    private volatile String lastGtidInQueue = StringUtils.EMPTY;

    private volatile GtidLogEvent lastGtidEventInQueue;

    private Future future;

    private Future eventFuture;

    public GtidConsumer(boolean executed) {
        if (executed) {
            startConsume();
        }
    }

    public GtidConsumer(boolean executed, boolean gtidEvent) {
        if (executed && gtidEvent) {
            startConsumeGtidEvent();
        }
    }

    public boolean add(String gtid) {
        return gtidSetString.add(gtid);
    }

    public boolean offer(GtidLogEvent gtidLogEvent) {
        return gtidEventQueue.offer(gtidLogEvent);
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
        synAddGtidSetString();
        logger.info("[Consume] gtid queue completely and return executed gtid {}", gtidSet);
        gtidSet.add(lastGtidInQueue);
        if (future != null) {
            future.cancel(true);
        }
        return gtidSet;
    }

    public GtidSet getGtidEventSet() {
        while (!gtidEventQueue.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
        }
        synAddGtidSetString();
        logger.info("[Consume] gtid event queue completely and return executed gtid {}", gtidSet);
        if (lastGtidEventInQueue != null) {
            gtidSet.add(lastGtidEventInQueue.getGtid());
        }
        if (eventFuture != null) {
            eventFuture.cancel(true);
        }
        return gtidSet;
    }

    private void synAddGtidSetString() {
        int size = gtidSetString.size();
        if (size > 0) {
            logger.info("[gtidSetString] size is {}", size);
            for (String gtid : gtidSetString) {
                gtidSet.add(gtid);
            }
            gtidSetString.clear();
        }
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

    private void startConsumeGtidEvent() {
        eventFuture = gtidService.submit(() -> {
            try {
                while (true) {
                    try {
                        lastGtidEventInQueue = gtidEventQueue.take();
                    } catch (InterruptedException e) {
                        break;
                    }
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.gtid.event.restore", "executed", new Task() {
                        @Override
                        public void go() {
                            gtidSet.add(lastGtidEventInQueue.getGtid());
                        }
                    });
                }
            } catch (Exception e) {
                logger.error("consume gtid event error", e);
            }
            logger.info("gtidService event finished and return");
        });
    }
}
