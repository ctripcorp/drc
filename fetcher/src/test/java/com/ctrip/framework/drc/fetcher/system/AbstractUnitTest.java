package com.ctrip.framework.drc.fetcher.system;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/9
 */
public class AbstractUnitTest {
    class GAQ extends AbstractResource {}
    class WorkerDock extends AbstractResource {};

    GAQ gaq = new GAQ();
    WorkerDock workerDock = new WorkerDock();
    TestSystem system;

    TestSystem systemWithoutConfig;

    class TestSystem extends AbstractSystem {
        public TestSystem() {
            resources.put("GAQ", gaq);
            resources.put("WorkerDock", workerDock);
        }
    }

    class UnitL1 extends AbstractUnit {
        @InstanceResource
        public GAQ gaq;
    }

    class UnitL2 extends UnitL1 {
        @InstanceResource
        public WorkerDock workerDock;

        @InstanceConfig(path="replicator.ip")
        public String replicatorIp = "unset";

        @InstanceConfig(path="id")
        public int id;

        @InstanceConfig(path="timestamp")
        public long timestamp;
    }

    public static class UnitL3 extends AbstractUnit {
        @Derived
        public WorkerDock workerDock;
    }

    class Config {
        public Replicator getReplicator() {
            return new Replicator();
        }
        public int getId() {
            return 1;
        }
        public long getTimestamp() {
            return 0;
        }
    }

    class Replicator {
        public String getIp() {
            return "127.0.0.1";
        }
    }

    @Before
    public void setUp() throws Exception {
        Config config = new Config();
        system = new TestSystem();
        system.setConfig(config, config.getClass());

        systemWithoutConfig = new TestSystem();
    }

    @Test
    public void testL1() throws Exception {
        UnitL1 unit = new UnitL1();
        unit.setSystem(system);
        assertNull(unit.gaq);
        unit.load();
        assertSame(gaq, unit.gaq);
    }

    @Test
    public void testL2() throws Exception {
        UnitL2 unit = new UnitL2();
        unit.setSystem(system);
        assertNull(unit.workerDock);
        assertNull(unit.gaq);
        unit.load();
        assertSame(workerDock, unit.workerDock);
        assertSame(gaq, unit.gaq);
    }

    @Test
    public void config() throws Exception {
        UnitL2 unit = new UnitL2();
        unit.setSystem(system);
        unit.load();
        assertEquals("127.0.0.1", unit.replicatorIp);
        assertEquals(1, unit.id);
        assertEquals(0, unit.timestamp);
    }

    @Test
    public void configUnset() throws Exception {
        UnitL2 unit = new UnitL2();
        unit.setSystem(systemWithoutConfig);
        unit.load();
        assertEquals("unset", unit.replicatorIp);
    }

    @Test
    public void derive() throws Exception {
        UnitL2 parent = new UnitL2();
        parent.setSystem(system);
        parent.load();
        UnitL3 child = (UnitL3) parent.derive(UnitL3.class);
        assertSame(parent.workerDock, child.workerDock);
    }
}