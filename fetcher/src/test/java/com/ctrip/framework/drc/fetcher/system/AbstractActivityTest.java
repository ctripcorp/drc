package com.ctrip.framework.drc.fetcher.system;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class AbstractActivityTest {

    AbstractActivity activity;
    MockResource MR0 = new MockResource();
    MockResource MR1 = new MockResource();

    @Before
    public void setUp() throws Exception {
        activity = new TestActivity();
    }

    @After
    public void tearDown() throws Exception {
        activity = null;
    }

    @Test
    public void getResource() {
        activity.setSystem(new TestSystem());
        assertTrue(activity.getResource("MR0") == MR0);
        assertTrue(activity.getResource("MR1") == MR1);
    }

    @Test
    public void whenDo() throws Exception {
        ((TestActivity) activity).assertInDoInitialize = (activity) -> {
            activity.isInitializing();
        };
        ((TestActivity) activity).assertInDoStart = (activity) -> {
            activity.isStarting();
        };
        ((TestActivity) activity).assertInDoStop = (activity) -> {
            activity.isStopping();
        };
        ((TestActivity) activity).assertInDoDispose = (activity) -> {
            activity.isDisposing();
        };
        activity.initialize();
        activity.isInitialized();
        activity.start();
        activity.isStarted();
        activity.stop();
        activity.isStopped();
        activity.dispose();
        activity.isDisposed();
    }

    @FunctionalInterface
    interface ActivityInternalAssert {
        void run(TestActivity activity);
    }

    class TestActivity extends AbstractActivity {

        ActivityInternalAssert assertInDoInitialize = null;
        ActivityInternalAssert assertInDoStart = null;
        ActivityInternalAssert assertInDoStop = null;
        ActivityInternalAssert assertInDoDispose = null;

        @Override
        public void doDispose() {
            if (assertInDoDispose != null) {
                assertInDoDispose.run(this);
            }
        }

        @Override
        public void doInitialize() {
            if (assertInDoInitialize != null) {
                assertInDoInitialize.run(this);
            }
        }

        @Override
        public void doStart() {
            if (assertInDoStart != null) {
                assertInDoStart.run(this);
            }
        }

        @Override
        public void doStop() {
            if (assertInDoStop != null) {
                assertInDoStop.run(this);
            }
        }

        //Do use the methods below out side of internals assert function.
        public Resource getResource(String name) {
            return super.getResource(name);
        }
    }

    class TestSystem extends AbstractSystem {

        public TestSystem() {
            resources.put("MR0", MR0);
            resources.put("MR1", MR1);
        }

    }
}