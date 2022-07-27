package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.event.transaction.BaseBeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.BeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.system.TaskActivity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2021/3/16
 */
public class GroupActivityTest extends MockTest {

    @Mock
    private Transaction transaction;

    @Mock
    private BeginEvent beginEvent;

    @Mock
    private TransactionEvent transactionEvent;

    @Mock
    private TaskActivity taskActivity;

    private ExecutorResource executor = new ExecutorResource();

    private GroupActivity groupActivity;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        groupActivity = new TestGroupActivity();
        groupActivity.link(taskActivity);

        groupActivity.executor = executor;
        executor.initialize();
    }

    @After
    public void tearDown() throws Exception {
        executor.dispose();
    }

    @Test
    public void testGroup() throws Exception {
        groupActivity.initialize();
        groupActivity.start();

        groupActivity.trySubmit(beginEvent);
        groupActivity.trySubmit(transactionEvent);
        TimeUnit.MILLISECONDS.sleep(50);
        verify(transaction, times(1)).append(transactionEvent);

        groupActivity.stop();
        groupActivity.dispose();
    }

    class TestGroupActivity extends GroupActivity {

        @Override
        protected Transaction getTransaction(BaseBeginEvent b) {
            return transaction;
        }

        @Override
        protected TransactionEvent getRollbackEvent() {
            return beginEvent;
        }
    }

}
