package com.ctrip.framework.drc.messenger.mq;

import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.system.AbstractSystem;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2025/1/17 16:39
 */
public class MqPositionResourceTest {


    private static final String ip = "127.0.0.1";
    private static final int port = 3306;
    private static final String username = "username";
    private static final String password = "password";
    private static final String initialGtidExecuted = "712c708a-2fa6-11eb-b7e5-98039ba92412:1-377";



    @Test
    public void testAddGitd() throws Exception {
        MqPositionResource mqPositionResource = getKafkaPositionResource();
        mqPositionResource.initialize();
        mqPositionResource.add(new Gtid("712c708a-2fa6-11eb-b7e5-98039ba92412:378"));
        Thread.sleep(100);
        Assert.assertEquals(new GtidSet(mqPositionResource.getCurrentPosition()), new GtidSet("712c708a-2fa6-11eb-b7e5-98039ba92412:1-378"));
        mqPositionResource.union(new GtidSet("a33ded23-6960-11eb-a8e0-fa163e02998c:1-100"));
        Thread.sleep(100);
        Assert.assertEquals(new GtidSet(mqPositionResource.getCurrentPosition()), new GtidSet("a33ded23-6960-11eb-a8e0-fa163e02998c:1-100,712c708a-2fa6-11eb-b7e5-98039ba92412:1-378"));
    }

    @Test
    public void testUpdatePositionInDb() throws Exception {
        MqPositionResource mqPositionResource = getKafkaPositionResource();
        mqPositionResource.initialize();
        mqPositionResource.persistPosition();
        Assert.assertEquals(SystemStatus.STOPPED, mqPositionResource.getSystem().getStatus());

        mqPositionResource.dispose();
    }

    private MqPositionResource getKafkaPositionResource() {
        MqPositionResource mqPositionResource = new MqPositionResource();
        mqPositionResource.ip = ip;
        mqPositionResource.port = port;
        mqPositionResource.username = username;
        mqPositionResource.password = password;
        mqPositionResource.registryKey = "registryKey";
        mqPositionResource.initialGtidExecuted = initialGtidExecuted;
        mqPositionResource.setSystem(new AbstractSystem());
        return mqPositionResource;
    }



}
