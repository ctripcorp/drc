package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @Author limingdong
 * @create 2021/3/9
 */
public class AbstractSystemTest {

    AbstractSystem system;

    @Before
    public void setUp() throws Exception {
        system = new TestSystem();
    }

    @After
    public void tearDown() throws Exception {
        system = null;
    }

    @Test
    public void initialize() throws Exception {
        system.initialize();
        assertTrue(system.isInitialized());
        for (Resource resource : system.resources.values()) {
            assertTrue(((MockResource) resource).initializeCalled == 1);
        }
        for (Activity activity : system.activities.values()) {
            assertTrue(((TestActivity) activity).initializeCalled == 1);
        }
    }

    @Test
    public void start() throws Exception {
        system.initialize();
        system.start();
        assertTrue(system.isStarted());
        for (Activity activity : system.activities.values()) {
            assertTrue(((TestActivity) activity).startCalled == 1);
        }
        //A AbstractSystem does not implement LifecycleController
        //and does not throw Exception when it starts from the beginning.
        //The same as the other lifecycle methods.
    }

    @Test
    public void stop() throws Exception {
        system.initialize();
        system.start();
        system.stop();
        assertTrue(system.isStopped());
        for (Activity activity : system.activities.values()) {
            assertTrue(((TestActivity) activity).stopCalled == 1);
        }
    }

    @Test
    public void dispose() throws Exception {
        system.initialize();
        system.dispose();
        assertTrue(system.isDisposed());
        for (Activity activity : system.activities.values()) {
            assertTrue(((TestActivity) activity).disposeCalled == 1);
        }
        for (Resource resource : system.resources.values()) {
            assertTrue(((MockResource) resource).disposeCalled == 1);
        }
    }

    @Test
    public void whenChangingPhase() {
        List<String> beginNames = Arrays.asList(
                Initializable.PHASE_NAME_BEGIN,
                Startable.PHASE_NAME_BEGIN,
                Stoppable.PHASE_NAME_BEGIN,
                Disposable.PHASE_NAME_BEGIN
        );
        for (String beginName : beginNames) {
            system.setPhaseName(beginName);
            assertTrue(!system.canInitialize());
            assertTrue(!system.canStart());
            assertTrue(!system.canStop());
            assertTrue(!system.canDispose());
        }
    }

    @Test
    public void whenDisposed() {
        //A system can initialize when disposed.
        system.setPhaseName(Disposable.PHASE_NAME_END);
        assertTrue(system.canInitialize());
        assertTrue(!system.canStart());
        assertTrue(!system.canStop());
        assertTrue(!system.canDispose());
    }

    @Test
    public void whenInitialized() {
        system.setPhaseName(Initializable.PHASE_NAME_END);
        assertTrue(!system.canInitialize());
        assertTrue(system.canStart());
        assertTrue(!system.canStop());
        assertTrue(system.canDispose());
    }

    @Test
    public void whenStarted() {
        system.setPhaseName(Startable.PHASE_NAME_END);
        assertTrue(!system.canInitialize());
        assertTrue(!system.canStart());
        assertTrue(system.canStop());
        assertTrue(!system.canDispose());
    }

    @Test
    public void whenStopped() {
        system.setPhaseName(Stoppable.PHASE_NAME_END);
        assertTrue(!system.canInitialize());
        assertTrue(system.canStart());
        assertTrue(!system.canStop());
        assertTrue(system.canDispose());
    }

    @Test
    public void getLifecycleState() {
        //assertTrue(system.getLifecycleState() == system);
    }

    @FunctionalInterface
    interface ActivityInternalAssert {
        void run(TestActivity activity);
    }

    class TestSystem extends AbstractSystem {

        //When system.dispose() is called, all activities should be disposed before resources.
        ActivityInternalAssert inDispose = (activity) -> {
            for (Resource resource : activity.getResources().values()) {
                assertTrue(((MockResource) resource).disposeCalled == 0);
            }
        };

        //When system.initialize() is called, all resources should be initialized before activities.
        ActivityInternalAssert inInitialized = (activity) -> {
            for (Resource resource : activity.getResources().values()) {
                assertTrue(((MockResource) resource).initializeCalled == 1);
            }
        };

        public TestSystem() {

            TestActivity MA0 = new TestActivity();
            MA0.assertInInitialize = inInitialized;
            MA0.assertInDispose = inDispose;
            MA0.setSystem(this);
            activities.put("MA0", MA0);

            TestActivity MA1 = new TestActivity();
            MA0.assertInInitialize = inInitialized;
            MA0.assertInDispose = inDispose;
            MA1.setSystem(this);
            activities.put("MA1", MA1);

            resources.put("MR0", new MockResource());
            resources.put("MR1", new MockResource());
            resources.put("MR2", new MockResource());
        }
    }

    class TestActivity extends AbstractUnit implements Activity {

        public int startCalled = 0;
        public int stopCalled = 0;
        public int disposeCalled = 0;
        public int initializeCalled = 0;
        public ActivityInternalAssert assertInInitialize = null;
        public ActivityInternalAssert assertInStart = null;
        public ActivityInternalAssert assertInStop = null;
        public ActivityInternalAssert assertInDispose = null;

        @Override
        public void start() {
            startCalled++;
            if (assertInStart != null) {
                assertInStart.run(this);
            }
        }

        @Override
        public void stop() {
            stopCalled++;
            if (assertInStop != null) {
                assertInStop.run(this);
            }
        }

        @Override
        public void dispose() {
            disposeCalled++;
            if (assertInDispose != null) {
                assertInDispose.run(this);
            }
        }

        @Override
        public void initialize() {
            initializeCalled++;
            if (assertInInitialize != null) {
                assertInInitialize.run(this);
            }
        }

        //Do use the methods below out side of internals assert function.
        public Resource getResource(String name) {
            return super.getResource(name);
        }

        public Map<String, Resource> getResources() {
            return super.getResources();
        }
    }

}