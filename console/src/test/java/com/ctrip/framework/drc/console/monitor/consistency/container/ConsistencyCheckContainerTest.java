package com.ctrip.framework.drc.console.monitor.consistency.container;

import com.ctrip.framework.drc.console.monitor.MockTest;
import com.ctrip.framework.drc.console.monitor.consistency.instance.InstanceConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static com.ctrip.framework.drc.console.monitor.consistency.instance.DefaultConsistencyCheckTest.BU;
import static com.ctrip.framework.drc.console.monitor.consistency.instance.DefaultConsistencyCheckTest.CLUSTER_NAME;

/**
 * Created by jixinwang on 2021/1/7
 */
public class ConsistencyCheckContainerTest extends MockTest {

    @InjectMocks
    private ConsistencyCheckContainer consistencyCheckContainer;

    @Mock
    private MonitorTableSourceProvider configService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(configService.getDataConsistentMonitorSwitch()).thenReturn("on");
    }

    @Test
    public void testAddAndRemoveConsistencyCheck() throws Exception {

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setCluster(CLUSTER_NAME);
        Endpoint srcEndpoint = new DefaultEndPoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        Endpoint dstEndpoint = new DefaultEndPoint(MYSQL_IP, DST_PORT, MYSQL_USER, MYSQL_PASSWORD);
        instanceConfig.setSrcEndpoint(srcEndpoint);
        instanceConfig.setDstEndpoint(dstEndpoint);

        ConsistencyEntity consistencyEntity = new ConsistencyEntity.Builder()
                .clusterAppId(null)
                .buName(BU)
                .srcDcName("shaoy")
                .destDcName("sharb")
                .clusterName(CLUSTER_NAME)
                .srcMysqlIp(MYSQL_IP)
                .srcMysqlPort(SRC_PORT)
                .destMysqlIp(MYSQL_IP)
                .destMysqlPort(DST_PORT)
                .build();
        instanceConfig.setConsistencyEntity(consistencyEntity);

        DelayMonitorConfig monitorConfig = new DelayMonitorConfig();
        monitorConfig.setSchema("drcmonitordb");
        monitorConfig.setTable("delaymonitor");
        monitorConfig.setKey(KEY);
        monitorConfig.setOnUpdate(ON_UPDATE);

        instanceConfig.setDelayMonitorConfig(monitorConfig);

        boolean ret = consistencyCheckContainer.addConsistencyCheck(instanceConfig);
        Assert.assertEquals(true, ret);

        consistencyCheckContainer.removeConsistencyCheck(TABLE);
    }
}
