package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.monitor.module.AbstractTestStarter;
import com.ctrip.framework.drc.monitor.module.replicate.ReplicatorApplierPairModule;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest.*;

/**
 *  *  run doTest to start integrity test WITHOUT any interference
 *  *
 *  *  start src and dst mysql instance by port 3306 and 3307 if they are not in use, or pick up the first free port starting from 3306
 *  *  with docker image 'mysql:5.7'
 *  *
 *  *  my.cnf is placed in src/my.cnf and dst/my.cnf respectively
 *  *
 *  *  to stop all containers
 * Created by mingdongli
 * 2019/10/15 上午1:10.
 */
public class BidirectionalStarter extends AbstractTestStarter {

    static {
        stopMysql();
    }

    private static final String MYSQL_STOP = "docker ps -a | grep mysql | awk '{print $1}' | grep -v CONTAINER | xargs docker rm -f";

    private static final String REPLICATOR_PATH = "/tmp/drc/";

    private ReplicatorApplierPairModule replicatorApplierPairModule;

    private int replicatorPortB = replicatorPortA + 1;

    private Set<String> includedDbs = Sets.newHashSet();   //inverse direction

    public boolean blockForManualTest = true;
    public boolean skipMonitor = false;
    public boolean startLocalSchemaManager = false;

    @Before
    public void setUp() {
        System.setProperty(SystemConfig.KEY_REPLICATOR_PATH, REPLICATOR_PATH);
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

    private static void stopMysql() {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command("bash", "-c", MYSQL_STOP);
            Process stop = processBuilder.start();
            int exitVal = stop.waitFor();
            if (exitVal == 0) {
                while (available(BASE_PORT) != BASE_PORT) {
                    TimeUnit.MILLISECONDS.sleep(500);  // port reuse
                }
                logger.info("[exec] {}, stop docker mysql success", MYSQL_STOP);
                return;
            }
            throw new RuntimeException("stop docker mysql error");
        } catch (Throwable t) {
            throw new RuntimeException("stop docker mysql error");
        }
    }
}
