package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest;
import com.ctrip.framework.drc.monitor.module.replicate.ReplicatorApplierPairModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mingdongli
 * 2019/10/15 下午5:22.
 */
public class RAStarter {

    private ReplicatorApplierPairModule replicatorApplierPairModule;

    private int srcMySQLPort = 3306;

    private int destMySQLPort = 3307;

    private int repPort = 8383;

    @Before
    public void setUp() {
        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(1024 * 1024 * 5)); //binlog文件大小
        System.setProperty(SystemConfig.BENCHMARK_SWITCH_TEST, String.valueOf(false));  //压测开关，默认关闭
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));  //循环检测通过show master status动态更新，集成测试使用该方式
    }

    @Test
    public void doTest() throws Exception {

        //启动双向RA
        replicatorApplierPairModule = new ReplicatorApplierPairModule(destMySQLPort, srcMySQLPort, repPort + 1, AbstractConfigTest.DESTINATION_REVERSE, RowFilterType.None, null);
        replicatorApplierPairModule.initialize();
        replicatorApplierPairModule.start();

        //启动测试模块

        Thread.currentThread().join();
    }

    @After
    public void tearDown() throws Exception {

        replicatorApplierPairModule.stop();
        replicatorApplierPairModule.dispose();
        replicatorApplierPairModule.destroy();
    }
}
