package com.ctrip.framework.drc.messenger.mq;

import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.transaction.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by shiruixin
 * 2024/11/8 16:51
 */
public class MqPositionResourceTest {
    private MqPositionResource mqPosition = new MqPositionResource();

    private ZkClient zkClient = mock(ZkClient.class);

    private CuratorFramework curatorFramework = mock(CuratorFramework.class);

    private ExistsBuilder existsBuilder = mock(ExistsBuilder.class);

    private static final String path = "/applier/config/positions/drc_dalcluster.mq_test._drc_mq";

    private static final String toUpdateGtid = "6afbad2c-fabe-11e9-878b-fa163eb626bd:1";

    private static final String ip = "127.0.0.1";
    private static final int port = 3306;
    private static final String username = "username";
    private static final String password = "password";

    @Before
    public void setUp() throws Exception {
        mqPosition.setZkClient(zkClient);
        when(zkClient.get()).thenReturn(curatorFramework);
        when(curatorFramework.checkExists()).thenReturn(existsBuilder);

        Stat stat = mock(Stat.class);
        when(existsBuilder.forPath(anyString())).thenReturn(stat);

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(curatorFramework.getData()).thenReturn(getDataBuilder);

        when(getDataBuilder.forPath(anyString())).thenReturn(toUpdateGtid.getBytes(StandardCharsets.UTF_8));

        CuratorTransaction curatorTransaction = mock(CuratorTransaction.class);
        when(curatorFramework.inTransaction()).thenReturn(curatorTransaction);

        TransactionCheckBuilder transactionCheckBuilder = mock(TransactionCheckBuilder.class);
        when(curatorTransaction.check()).thenReturn(transactionCheckBuilder);

        CuratorTransactionBridge curatorTransactionBridge = mock(CuratorTransactionBridge.class);
        when(transactionCheckBuilder.forPath(path)).thenReturn(curatorTransactionBridge);

        CuratorTransactionFinal curatorTransactionFinal = mock(CuratorTransactionFinal.class);
        when(curatorTransactionBridge.and()).thenReturn(curatorTransactionFinal);

        TransactionSetDataBuilder transactionSetDataBuilder = mock(TransactionSetDataBuilder.class);
        when(curatorTransactionFinal.setData()).thenReturn(transactionSetDataBuilder);

        when(transactionSetDataBuilder.forPath(path, toUpdateGtid.getBytes())).thenReturn(curatorTransactionBridge);

        when(curatorTransactionBridge.and()).thenReturn(curatorTransactionFinal);

        mqPosition.registryKey = "drc_dalcluster.mq_test._drc_mq";
        mqPosition.ip = ip;
        mqPosition.port = port;
        mqPosition.username = username;
        mqPosition.password = password;
        mqPosition.doInitialize();
    }

    @After
    public void tearDown() throws Exception {
        mqPosition.doDispose();
    }

    @Test
    public void updatePosition() {
        mqPosition.add(new Gtid(toUpdateGtid));
        String position = mqPosition.getCurrentPosition();
        Assert.assertEquals(toUpdateGtid, position);
    }

    @Test
    public void getPosition() {
        String position = mqPosition.get();
        Assert.assertEquals(toUpdateGtid, position);
    }
}