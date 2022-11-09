package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

public class MessengerServiceImplTest {

    @InjectMocks
    private MessengerServiceImpl messengerService;

    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;

    @Mock
    private MessengerTblDao messengerTblDao;

    @Mock
    private ResourceTblDao resourceTblDao;

    @Mock
    private DataMediaPairService dataMediaPairService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        MessengerGroupTbl messengerGroupTbl = mockMessengerGroupTbl();
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(1L),Mockito.anyInt())).thenReturn(messengerGroupTbl);

        MessengerProperties messengerProperties = mockMessengerProperties();
        Mockito.when(dataMediaPairService.generateMessengerProperties(Mockito.eq(1L))).thenReturn(messengerProperties);

        List<MessengerTbl> messengerTbls = mockMessengerTbls();
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(1L))).thenReturn(messengerTbls);

        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setIp("ip1");
        Mockito.when( resourceTblDao.queryByPk(Mockito.eq(1L))).thenReturn(resourceTbl);

    }


    /**
     * {
     *     "mqConfigs": [
     *         {
     *             "mqType": "qmq",
     *             "table": "db1.t1",
     *             "topic": "db1.t1.drc.changed.topic",
     *             "serialization": "json",
     *             "persistent": true,
     *             "persistentDb": "dalClusterName",
     *             "order": true,
     *             "orderKey": "orderKeyName",
     *             "delayTime": 1111,
     *             "processor": "java file content"
     *         }
     *     ]
     * }
     *
     */

    @Test
    public void testGenerateMessengers() throws SQLException {
        List<Messenger> messengers = messengerService.generateMessengers(1L);
        Assert.assertEquals(1,messengers.size());
        System.out.println(messengers.get(0).getProperties());
    }


    private MessengerGroupTbl mockMessengerGroupTbl() {
        MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
        messengerGroupTbl.setId(1L);
        messengerGroupTbl.setGtidExecuted("uuid1:1-10");
        return messengerGroupTbl;
    }

    private MessengerProperties mockMessengerProperties() {
        MessengerProperties messengerProperties = new MessengerProperties();
        MqConfig mqConfig = new MqConfig();
        mqConfig.setMqType("qmq");
        mqConfig.setTable("db1.t1");
        mqConfig.setTopic("db1.t1.drc.changed.topic");
        mqConfig.setSerialization("json");
        mqConfig.setPersistent(true);
        mqConfig.setPersistentDb("dalClusterName");
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("orderKeyName");
        mqConfig.setDelayTime(1111);
        mqConfig.setProcessor("java file content");
        messengerProperties.setMqConfigs(Lists.newArrayList(mqConfig));
        return messengerProperties;
    }

    private List<MessengerTbl> mockMessengerTbls() {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setResourceId(1L);
        messengerTbl.setPort(8080);
        return Lists.newArrayList(messengerTbl);
    }
}
