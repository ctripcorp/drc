package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class AbstractLoopActivityTest {

    @Test
    public void simpleUse() throws Exception {

        class TestLoopActivity extends AbstractLoopActivity {

            @Override
            public void doInitialize() {
                ExecutorResource tpe = new ExecutorResource();
                try {
                    tpe.initialize();
                } catch (Exception e) {
                }
                executor = tpe;
            }

            @Override
            public void loop() {
            }
        }

        Activity activity = new TestLoopActivity();
        activity.initialize();
        activity.start();
        Thread.sleep(100);
        activity.stop();
        activity.dispose();
    }

}