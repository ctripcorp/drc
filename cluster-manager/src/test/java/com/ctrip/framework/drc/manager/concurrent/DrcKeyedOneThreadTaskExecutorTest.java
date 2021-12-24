package com.ctrip.framework.drc.manager.concurrent;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.MockTest;
import com.ctrip.framework.drc.manager.healthcheck.notifier.AbstractNotifier;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @Author limingdong
 * @create 2021/1/21
 */
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class DrcKeyedOneThreadTaskExecutorTest extends MockTest {

    private DrcKeyedOneThreadTaskExecutor drcKeyedOneThreadTaskExecutor;

    private Executor executors = ThreadUtils.newSingleThreadExecutor("DrcKeyedOneThreadTaskExecutorTest");

    private Command command;

    private Command failCommand;

    @Mock
    private Throwable throwable;

    private AtomicInteger scount = new AtomicInteger(0);

    private AtomicInteger fcount = new AtomicInteger(0);

    private int successCount = 3;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        drcKeyedOneThreadTaskExecutor = new DrcKeyedOneThreadTaskExecutor(executors);
        command = new AbstractCommand() {
            @Override
            protected void doExecute() throws Exception {
                scount.addAndGet(1);
                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "test";
            }
        };

        failCommand = new AbstractCommand() {
            @Override
            protected void doExecute() throws Exception {  //retry time = interval * retryCount
                fcount.addAndGet(1);
                System.out.println("execute fcount " + fcount.get());
                if (fcount.get() == successCount) {
                    future().setSuccess();
                } else {
                    future().setFailure(throwable);
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        drcKeyedOneThreadTaskExecutor.destroy();
    }

    @Test
    public void test_01_Success() {
        String key = RandomStringUtils.randomAlphabetic(10);
        drcKeyedOneThreadTaskExecutor.execute(key, command);
        drcKeyedOneThreadTaskExecutor.execute(key, command);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        Assert.assertEquals(2 , scount.get());
    }

    @Test
    public void test_02_Fail() {
        scount.set(0);
        fcount.set(0);

        AbstractNotifier.RETRY_INTERVAL = 20;

        String key = RandomStringUtils.randomAlphabetic(10);
        drcKeyedOneThreadTaskExecutor.execute(key, failCommand);
        drcKeyedOneThreadTaskExecutor.execute(key, command);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250)); // 0, 20, 40, (60)
        Assert.assertEquals(1 , scount.get());
        Assert.assertEquals(successCount , fcount.get());
    }
}