package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.resource.thread.Executor;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public abstract class AbstractLoopActivity extends AbstractActivity implements Runnable {

    @InstanceResource
    public Executor executor;

    private volatile boolean stopping = false;

    @Override
    public void doStart() {
        executor.execute(this);
        stopping = false;
    }

    @Override
    public void doStop() {
        stopping = true;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(getSystemName() + "-" + getClass().getSimpleName() + "-" + hashCode());
        try {
            while (!stopping) {
                try {
                    loop();
                } catch (InterruptedException e) {
                    logger.info(getClass().getSimpleName() + " - QUIT stopping: {}", stopping);
                } catch (Throwable e) {
                    logger.error(getClass().getSimpleName() + " - UNLIKELY: ", e);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        logger.info(ex.getMessage());
                    }
                }
            }
        } finally {
            releaseAfterStop();
        }
    }

    protected void releaseAfterStop() {
    }

    public abstract void loop() throws InterruptedException;

}
