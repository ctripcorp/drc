package com.ctrip.framework.drc.service.config;

import com.ctrip.framework.drc.fetcher.system.AbstractSystem;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.qconfig.AbstractUnitUsingQConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class QConfigSourceTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ExecutorService executorService = Executors.newFixedThreadPool(60);

    private ByteArrayOutputStream baos;

    class TestSystem extends AbstractSystem {

    }

    @Before
    public void init() {
        baos = new ByteArrayOutputStream(1024);
        PrintStream cacheStream = new PrintStream(baos);
        System.setOut(cacheStream);
    }

    @After
    public void tearDown() throws IOException {
        System.setOut(System.out);
        baos.close();
    }

    class QConfigUnit1 extends AbstractUnitUsingQConfig {

        @InstanceConfig(path = "number1")
        public volatile int number1 = 1;

        @InstanceConfig(path = "number2")
        public volatile int number2 = 1;

        @Override
        public String namespace() {
            return "test1";
        }
    }

    class QConfigUnit2 extends AbstractUnitUsingQConfig {

        @InstanceConfig(path = "number3")
        public volatile int number3 = 1;

        @Override
        public String namespace() {
            return "test2";
        }
    }

    @Test
    public void testBind() throws Exception {
        QConfigUnit1 unit1 = new QConfigUnit1();
        unit1.setSystem(new TestSystem());
        unit1.load();

        QConfigUnit1 unit2 = new QConfigUnit1();
        unit2.setSystem(new TestSystem());
        unit2.load();

        QConfigUnit2 unit3 = new QConfigUnit2();
        unit3.setSystem(new TestSystem());
        unit3.load();

        for (int i = 0; i < 60; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    QConfigUnit1 unit1 = new QConfigUnit1();
                    unit1.setSystem(new TestSystem());
                    try {
                        unit1.load();
                    } catch (Exception e) {
                        logger.error("QConfig load test error", e);
                    }
                }
            });
        }

        Assert.assertEquals(10, unit1.number1);
        Assert.assertEquals(20, unit1.number2);
        Assert.assertEquals(30, unit3.number3);

        String message = baos.toString();
        Assert.assertEquals(false, message.contains("title=qconfig,app=100023500"));
    }
}
