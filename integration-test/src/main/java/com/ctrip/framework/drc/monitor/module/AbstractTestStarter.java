package com.ctrip.framework.drc.monitor.module;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest;
import com.ctrip.framework.drc.monitor.module.replicate.UnidirectionalReplicateModule;

import java.io.File;

import static com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest.*;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_PATH;

/**
 * Created by mingdongli
 * 2019/10/15 上午11:06.
 */
public abstract class AbstractTestStarter {

    protected UnidirectionalReplicateModule unidirectionalReplicateModule;

    protected int mysqlPortA = SOURCE_MASTER_PORT;
    protected int mysqlPortB = DESTINATION_MASTER_PORT;
    protected int mysqlPortMeta = META_PORT;
    protected int replicatorPortA = REPLICATOR_MASTER_PORT;

    public void setUp() {
        System.setProperty(SystemConfig.BENCHMARK_SWITCH_TEST, String.valueOf(false));  //压测开关，默认关闭
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));  //循环检测通过show variables like ""动态更新，集成测试使用该方式
        System.setProperty(SystemConfig.MYSQL_LOCAL_INSTANCE_TEST, String.valueOf(true));
        System.setProperty(SystemConfig.MYSQL_DB_INIT_TEST, String.valueOf(true));

        cleanUp();
        try {
            unidirectionalReplicateModule = getUnidirectionalReplicateModule(mysqlPortA, mysqlPortB, mysqlPortMeta, replicatorPortA, REGISTRY_KEY);
            unidirectionalReplicateModule.initialize();
            unidirectionalReplicateModule.start();  //just for phase necessary
        } catch (Exception e) {
        }
    }

    protected UnidirectionalReplicateModule getUnidirectionalReplicateModule(int mysqlPortA, int mysqlPortB, int mysqlPortMeta, int replicatorPortA, String registerKey) {
        return new UnidirectionalReplicateModule(mysqlPortA, mysqlPortB, mysqlPortMeta, replicatorPortA, registerKey);
    }

    public abstract void doTest() throws Exception;

    public void tearDown() throws Exception {
        unidirectionalReplicateModule.stop();
        unidirectionalReplicateModule.dispose();
        unidirectionalReplicateModule.destroy();
    }

    private void cleanUp() {
        doCleanUp(AbstractConfigTest.REGISTRY_KEY + "." + MHA_NAME);
        doCleanUp(AbstractConfigTest.DESTINATION_REVERSE + "." + MHA_NAME);
    }

    private void doCleanUp(String destination) {
        File logDir = new File(LOG_PATH + destination);
        File[] files = logDir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            file.delete();
        }
    }
}
