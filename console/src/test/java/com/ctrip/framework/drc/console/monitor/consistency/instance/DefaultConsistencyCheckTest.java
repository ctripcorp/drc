package com.ctrip.framework.drc.console.monitor.consistency.instance;

import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;
import com.ctrip.framework.drc.console.enums.SourceTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * Created by mingdongli
 * 2019/11/19 下午3:30.
 */
public class DefaultConsistencyCheckTest {

    public static final String CLUSTER_NAME = "drc_unit_test";

    public static final String BU = "平台研发中心";

    private DefaultConsistencyCheck consistencyCheck;

    private DalUtils dalUtils = DalUtils.getInstance();

    @Before
    public void setUp() throws Exception {
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
                .mhaName("mhaName1")
                .build();
        instanceConfig.setConsistencyEntity(consistencyEntity);

        DelayMonitorConfig monitorConfig = new DelayMonitorConfig();
        monitorConfig.setSchema("drcmonitordb");
        monitorConfig.setTable("delaymonitor");
        monitorConfig.setKey(KEY);
        monitorConfig.setOnUpdate(ON_UPDATE);

        instanceConfig.setDelayMonitorConfig(monitorConfig);

        consistencyCheck = new DefaultConsistencyCheck(instanceConfig);
        consistencyCheck.initialize();
        consistencyCheck.start();
    }

    @Test
    public void check() {
        boolean res = consistencyCheck.check();
        Assert.assertFalse(res);
    }

    @Test
    public void testRecordInconsistency() throws SQLException {
        Set<String> currentKeys = Sets.newHashSet("300", "301");
        consistencyCheck.recordInconsistency(currentKeys, SourceTypeEnum.INCREMENTAL);
        DataInconsistencyHistoryTbl sample = new DataInconsistencyHistoryTbl();
        sample.setMonitorTableKeyValue("300");
        List<DataInconsistencyHistoryTbl> dataInconsistencyHistoryTblList = dalUtils.getDataInconsistencyHistoryTblDao().queryBy(sample);
        DataInconsistencyHistoryTbl result = dataInconsistencyHistoryTblList.get(0);
        Assert.assertEquals("drcmonitordb", result.getMonitorSchemaName());
        Assert.assertEquals("delaymonitor", result.getMonitorTableName());
    }
}
