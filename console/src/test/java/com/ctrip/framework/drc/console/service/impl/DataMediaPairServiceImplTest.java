package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DataMediaPairTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.core.meta.MessengerProperties;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
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


public class DataMediaPairServiceImplTest {

    @InjectMocks
    private DataMediaPairServiceImpl dataMediaPairService;

    @Mock
    private DataMediaPairTblDao dataMediaPairTblDao;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        
        List<DataMediaPairTbl> dataMediaPairTbls = mockDataMediaPairTbls();
        Mockito.when(dataMediaPairTblDao.queryByGroupId(Mockito.eq(1L))).thenReturn(dataMediaPairTbls);

    }

    @Test
    public void testGenerateMqConfigs() throws SQLException {
        MessengerProperties messengerProperties = dataMediaPairService.generateMessengerProperties(1L);
        Assert.assertEquals(1,messengerProperties.getMqConfigs().size());
        System.out.println(JsonUtils.toJson(messengerProperties.getMqConfigs().get(0)));
        /**
         * {
         *     "mqType": "qmq",
         *     "table": "db1.t1",
         *     "topic": "db1.t1.drc.changed.topic",
         *     "serialization": "json",
         *     "persistent": true,
         *     "persistentDb": "dalClusterName",
         *     "order": true,
         *     "orderKey": "orderKeyName",
         *     "delayTime": 1111,
         *     "processor": "java file content"
         * }
         */
    }
    
    private List<DataMediaPairTbl> mockDataMediaPairTbls() {
        DataMediaPairTbl dataMediaPairTbl = new DataMediaPairTbl();
        dataMediaPairTbl.setSrcDataMediaName("db1.t1");
        dataMediaPairTbl.setDestDataMediaName("db1.t1.drc.changed.topic");
        dataMediaPairTbl.setProcessor("java file content");
        dataMediaPairTbl.setProperties("{\n" +
                "            \"mqType\": \"qmq\",\n" +
                "            \"serialization\": \"json\",\n" +
                "            \"persistent\": true,\n" +
                "            \"persistentDb\": \"dalClusterName\",\n" +
                "            \"order\": true,\n" +
                "            \"orderKey\": \"orderKeyName\",\n" +
                "            \"delayTime\": 1111\n" +
                "        }");
        return Lists.newArrayList(dataMediaPairTbl);

    }
    
}