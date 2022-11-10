package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class RetryTaskTest {

    private AtomicInteger count = new AtomicInteger(0);

    @Test
    public void testCall() {
        int res = new RetryTask<>(new TestCallable()).call();
        Assert.assertEquals(res, 1);
    }

    @Test
    public void testCallException() {
        Integer res = new RetryTask<>(new ExceptionCallable()).call();
        Assert.assertEquals(res, null);
    }

    class TestCallable implements NamedCallable<Integer> {

        @Override
        public Integer call() {
            return count.incrementAndGet();
        }
    }

    class ExceptionCallable implements NamedCallable<Integer> {

        @Override
        public Integer call() {
            throw new RuntimeException("test");
        }

        @Override
        public void afterException(Throwable t){
            DDL_LOGGER.error("TEST LOG {} error", name(), t);
        }
    }
}
