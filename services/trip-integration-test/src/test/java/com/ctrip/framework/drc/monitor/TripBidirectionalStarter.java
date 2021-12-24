package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.monitor.module.AbstractTestStarter;
import com.ctrip.framework.drc.monitor.module.TripUnidirectionalReplicateModule;
import com.ctrip.framework.drc.monitor.module.replicate.ReplicatorApplierPairModule;
import com.ctrip.framework.drc.monitor.module.replicate.UnidirectionalReplicateModule;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest.DESTINATION_REVERSE;
import static com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest.REGISTRY_KEY;

/**
 *  *  run doTest to start integrity test WITHOUT any interference
 *  *
 *  *  start src and dst mysql instance by port 3306 and 3307 if they are not in use, or pick up the first free port starting from 3306
 *  *  with docker image 'mysql:5.7'
 *  *
 *  *  my.cnf is placed in src/my.cnf and dst/my.cnf respectively
 *  *
 *  *  docker ps -a | grep mysql | awk '{print $1}' | grep -v CONTAINER | xargs docker rm -f
 *  *  to stop all containers
 * Created by mingdongli
 * 2019/10/15 上午1:10.
 */
public class TripBidirectionalStarter extends AbstractTestStarter {

    private ReplicatorApplierPairModule replicatorApplierPairModule;

    private int replicatorPortB = replicatorPortA + 1;

    private Set<String> includedDbs = Sets.newHashSet("drc1");   //inverse direction

    public boolean blockForManualTest = true;
    public boolean skipMonitor = false;
    public boolean startLocalSchemaManager = false;

    protected UnidirectionalReplicateModule getUnidirectionalReplicateModule(int mysqlPortA, int mysqlPortB, int mysqlPortMeta, int replicatorPortA, String registerKey) {
        return new TripUnidirectionalReplicateModule(mysqlPortA, mysqlPortB, mysqlPortMeta, replicatorPortA, registerKey);
    }

    @Before
    public void setUp() {
        super.setUp();
        if (startLocalSchemaManager) {
            System.setProperty(SystemConfig.REPLICATOR_LOCAL_SCHEMA_MANAGER, String.valueOf(true));
        }
    }

    @Test
    public void doTest() throws Exception {
        //启动单向MySQL、初试化表、RA
        unidirectionalReplicateModule.startMySQLModule();
        unidirectionalReplicateModule.startRAModule();

        //启动双向RAEventTransactionCache.java
        if (DESTINATION_REVERSE.equalsIgnoreCase(REGISTRY_KEY)) {
            System.setProperty(SystemConfig.REVERSE_REPLICATOR_SWITCH_TEST, String.valueOf(true));
        }
        replicatorApplierPairModule = new ReplicatorApplierPairModule(mysqlPortB, mysqlPortA, replicatorPortB, DESTINATION_REVERSE);
        replicatorApplierPairModule.setIncludedDb(includedDbs);
        replicatorApplierPairModule.initialize();
        replicatorApplierPairModule.start();

        if (!skipMonitor) {
            unidirectionalReplicateModule.startMonitorModule();
        }

        if (blockForManualTest) {
            Thread.currentThread().join();
        }
    }

    @After
    public void tearDown() throws Exception {
        replicatorApplierPairModule.stop();
        replicatorApplierPairModule.dispose();
        replicatorApplierPairModule.destroy();
        if (!skipMonitor) {
            unidirectionalReplicateModule.stopMonitorModule();
        }
        super.tearDown();
    }
}
