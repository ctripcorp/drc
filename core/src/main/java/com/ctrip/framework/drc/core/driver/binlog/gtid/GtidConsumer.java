package com.ctrip.framework.drc.core.driver.binlog.gtid;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.concurrent.*;

/**
 * @Author limingdong
 * @create 2019/12/31
 */
public class GtidConsumer {

    private static final Logger logger = LoggerFactory.getLogger(GtidConsumer.class);

    private GtidSet gtidSet = new GtidSet(new LinkedHashMap<>());

    private ExecutorService gtidService = Executors.newCachedThreadPool(new NamedThreadFactory("Gtid-Consume"));

    private BlockingQueue<String> gtidQueue = new LinkedBlockingDeque<String>();

    private volatile boolean threadFinished = false;

    public GtidConsumer(boolean executed) {
        if (executed) {
            startConsume();
        }
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

        logger.info("[Consume] gtid queue completely and return executed gtid {}", gtidSet);
        threadFinished = true;
        return gtidSet;
    }

    private void startConsume() {
        gtidService.submit(() -> {
            try {
                while (!threadFinished) {
                    String gtid = gtidQueue.poll(5, TimeUnit.SECONDS);
                    if (gtid == null) {
                        continue;
                    }
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.gtid.restore", "executed", new Task() {
                        @Override
                        public void go() {
                            gtidSet.add(gtid);
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
