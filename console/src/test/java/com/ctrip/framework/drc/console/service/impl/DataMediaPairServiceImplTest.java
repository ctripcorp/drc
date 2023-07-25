package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DataMediaPairTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
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
    
    @Mock
    private RowsFilterService rowsFilterService;

    @Mock
    private DrcDoubleWriteService drcDoubleWriteService;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(defaultConsoleConfig.getDrcDoubleWriteSwitch()).thenReturn("off");

        List<DataMediaPairTbl> dataMediaPairTbls = mockDataMediaPairTbls();
        Mockito.when(dataMediaPairTblDao.queryByGroupId(Mockito.eq(1L))).thenReturn(dataMediaPairTbls);
        Mockito.when(rowsFilterService.generateRowsFiltersConfig(Mockito.eq(1L),Mockito.eq(0))).
                thenReturn(Lists.newArrayList());
    }

    @Test
    public void testGenerateMessengerProperties() throws SQLException {
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
    
    @Test
    public void testAddMqConfig() throws Exception {
        Mockito.when(dataMediaPairTblDao.insertWithReturnId(Mockito.any(DataMediaPairTbl.class))).thenReturn(1L);
        MqConfigDto mqConfigDto = generateDto();
        Mockito.doNothing().when(drcDoubleWriteService).insertDbReplicationForMq(Mockito.anyLong());
        Assert.assertEquals("addMqConfig success",dataMediaPairService.addMqConfig(mqConfigDto));
    }

    @Test
    public void testUpdateMqConfig() throws Exception {
        Mockito.when(dataMediaPairTblDao.update(Mockito.any(DataMediaPairTbl.class))).thenReturn(1);
        MqConfigDto mqConfigDto = generateDto();
        Mockito.doNothing().when(drcDoubleWriteService).deleteDbReplicationForMq(Mockito.anyLong());
        Mockito.doNothing().when(drcDoubleWriteService).insertDbReplicationForMq(Mockito.anyLong());
        Assert.assertEquals("updateMqConfig success",dataMediaPairService.updateMqConfig(mqConfigDto));
    }

    @Test
    public void testDeleteMqConfig() throws Exception {
        Mockito.when(dataMediaPairTblDao.update(Mockito.any(DataMediaPairTbl.class))).thenReturn(1);
        Mockito.doNothing().when(drcDoubleWriteService).deleteDbReplicationForMq(Mockito.anyLong());
        Assert.assertEquals("deleteMqConfig success",dataMediaPairService.deleteMqConfig(1L));
    }

    @Test
    public void testGetDataMediaPairs() throws SQLException {
        Mockito.when(dataMediaPairTblDao.queryByGroupId(Mockito.anyLong())).thenReturn(null);
        dataMediaPairService.getPairsByMGroupId(1L);
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
    
    private MqConfigDto generateDto(){
        MqConfigDto mqConfigDto = new MqConfigDto();
        mqConfigDto.setId(0);
        mqConfigDto.setBu("fx");
        mqConfigDto.setMqType("qmq");
        mqConfigDto.setTable("db1\\.t1");
        mqConfigDto.setTopic("dr");
        mqConfigDto.setSerialization("json");
        mqConfigDto.setPersistent(false);
        mqConfigDto.setPersistentDb(null);
        mqConfigDto.setOrder(true);
        mqConfigDto.setOrderKey("id");
        mqConfigDto.setDelayTime(0L);
        mqConfigDto.setProcessor(null);
        mqConfigDto.setMessengerGroupId(1L);
        mqConfigDto.setMhaName("mha1");
        return mqConfigDto;
    }
    
}
