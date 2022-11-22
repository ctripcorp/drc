package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.vo.MessengerVo;
import com.ctrip.framework.drc.console.vo.MqConfigVo;
import com.ctrip.framework.drc.console.vo.response.QmqApiResponse;
import com.ctrip.framework.drc.console.vo.response.QmqBuEntity;
import com.ctrip.framework.drc.console.vo.response.QmqBuList;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.List;

public class MessengerServiceImplTest {

    @InjectMocks
    private MessengerServiceImpl messengerService;

    @Mock private DataMediaPairService dataMediaPairService;

    @Mock private MhaService mhaService;

    @Mock private DomainConfig domainConfig;

    @Mock private MessengerGroupTblDao messengerGroupTblDao;

    @Mock private MessengerTblDao messengerTblDao;

    @Mock private ResourceTblDao resourceTblDao;

    @Mock private MhaTblDao mhaTblDao;

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

    @Test
    public void testGenerateMessengers() throws SQLException {
        List<Messenger> messengers = messengerService.generateMessengers(1L);
        Assert.assertEquals(1,messengers.size());
        System.out.println(messengers.get(0).getProperties());
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
    public void testGetMessengerIps() throws SQLException {
        List<String> messengerIps = messengerService.getMessengerIps(1L);
        Assert.assertEquals(1,messengerIps.size());
        Assert.assertEquals("ip1",messengerIps.get(0));
    }

    @Test
    public void testGetMqConfigVos() throws SQLException {
        Mockito.when(dataMediaPairService.getDataMediaPairs(Mockito.anyLong())).thenReturn(mockDataMediaPairTbls());
        
        List<MqConfigVo> mqConfigVos = messengerService.getMqConfigVos(1L);
        Assert.assertEquals(1,mqConfigVos.size());
        Assert.assertEquals("db\\.table",mqConfigVos.get(0).getTable());
        Assert.assertEquals("topicName",mqConfigVos.get(0).getTopic());
    }

    @Test
    public void testGetBusFromQmq() {
        Mockito.when(domainConfig.getQmqBuListUrl()).thenReturn("url");
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(HttpUtils.post(Mockito.anyString(),Mockito.any(),Mockito.any())).thenReturn(mockQmqBuList());

            List<String> busFromQmq = messengerService.getBusFromQmq();
            Assert.assertEquals("fx",busFromQmq.get(0));
            Assert.assertEquals("flight",busFromQmq.get(1));
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testProcessAddMqConfig() throws Exception {
        Mockito.when(mhaService.getDcNameForMha(Mockito.eq("mha1"))).thenReturn("shaxy");
        Mockito.when(domainConfig.getQmqTopicApplicationUrl(Mockito.eq("shaxy"))).thenReturn("qmqTopicUrl");
        Mockito.when(domainConfig.getQmqProducerApplicationUrl(Mockito.eq("shaxy"))).thenReturn("qmqProducerUrl");
        Mockito.when(dataMediaPairService.addMqConfig(Mockito.any(MqConfigDto.class))).thenReturn("addMqConfig success");
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            QmqApiResponse qmqApiResponse = new QmqApiResponse();
            qmqApiResponse.setStatus(0);
            theMock.when(HttpUtils.post(Mockito.eq("qmqTopicUrl"),Mockito.any(),Mockito.any())).thenReturn(qmqApiResponse);
            qmqApiResponse.setStatus(1);
            qmqApiResponse.setStatusMsg("producer already existed");
            theMock.when(HttpUtils.post(Mockito.eq("qmqProducerUrl"),Mockito.any(),Mockito.any())).thenReturn(qmqApiResponse);
            String s = messengerService.processAddMqConfig(mockMqConfigDto());
            Assert.assertEquals("addMqConfig success",s);
        }
    }

    @Test
    public void testProcessUpdateMqConfig() throws Exception {
        Mockito.when(mhaService.getDcNameForMha(Mockito.eq("mha1"))).thenReturn("shaxy");
        Mockito.when(domainConfig.getQmqTopicApplicationUrl(Mockito.eq("shaxy"))).thenReturn("qmqTopicUrl");
        Mockito.when(domainConfig.getQmqProducerApplicationUrl(Mockito.eq("shaxy"))).thenReturn("qmqProducerUrl");
        Mockito.when(dataMediaPairService.updateMqConfig(Mockito.any(MqConfigDto.class))).thenReturn("updateMqConfig success");
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            QmqApiResponse qmqApiResponse = new QmqApiResponse();
            qmqApiResponse.setStatus(0);
            theMock.when(HttpUtils.post(Mockito.eq("qmqTopicUrl"),Mockito.any(),Mockito.any())).thenReturn(qmqApiResponse);
            theMock.when(HttpUtils.post(Mockito.eq("qmqProducerUrl"),Mockito.any(),Mockito.any())).thenReturn(qmqApiResponse);
            String s = messengerService.processUpdateMqConfig(mockMqConfigDto());
            Assert.assertEquals("updateMqConfig success",s);
        }
    }

    @Test
    public void testProcessDeleteMqConfig() throws Exception {
        Mockito.when(dataMediaPairService.deleteMqConfig(1L)).thenReturn("deleteMqConfig success");
        String s = messengerService.processDeleteMqConfig(1L);
        Assert.assertEquals("deleteMqConfig success",s);
    }

    @Test
    public void testGetAllMessengerVos() throws SQLException {
        Mockito.when(messengerGroupTblDao.queryBy(Mockito.any(MessengerGroupTbl.class))).
                thenReturn(Lists.newArrayList(mockMessengerGroupTbl()));
        MhaDto mhaDto = new MhaDto();
        mhaDto.setMhaName("mha1");
        mhaDto.setBuName("fx");
        mhaDto.setMonitorSwitch(1);
        Mockito.when(mhaService.queryMhaInfo(Mockito.eq(1L))).thenReturn(mhaDto);

        List<MessengerVo> allMessengerVos = messengerService.getAllMessengerVos();
        Assert.assertEquals(1,allMessengerVos.size());
    }

    


    private MessengerGroupTbl mockMessengerGroupTbl() {
        MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
        messengerGroupTbl.setId(1L);
        messengerGroupTbl.setMhaId(1L);
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

    private List<DataMediaPairTbl> mockDataMediaPairTbls() {
        DataMediaPairTbl dataMediaPairTbl = new DataMediaPairTbl();
        dataMediaPairTbl.setId(1L);
        dataMediaPairTbl.setType(1);
        dataMediaPairTbl.setGroupId(1L);
        dataMediaPairTbl.setSrcDataMediaName("db\\.table");
        dataMediaPairTbl.setDestDataMediaName("topicName");
        dataMediaPairTbl.setProperties(JsonUtils.toJson(mockMessengerProperties().getMqConfigs().get(0)));
        return Lists.newArrayList(dataMediaPairTbl);
    }
    
    private QmqBuList mockQmqBuList() {
        QmqBuList response = new QmqBuList();
        QmqBuEntity qmqBuEntity0 = new QmqBuEntity();
        qmqBuEntity0.setCnName("框架");
        qmqBuEntity0.setEnName("FX");
        QmqBuEntity qmqBuEntity1 = new QmqBuEntity();
        qmqBuEntity1.setCnName("机票");
        qmqBuEntity1.setEnName("flight");
        response.setStatus(0);
        response.setData(Lists.newArrayList(qmqBuEntity0,qmqBuEntity1));
        return response;
    }
    
    private MqConfigDto mockMqConfigDto () {
        MqConfigDto dto = new MqConfigDto();
        dto.setId(1L);
        dto.setBu("fx");
        dto.setMqType("qmq");
        dto.setTable("db\\.table");
        dto.setTopic("topicName");
        dto.setSerialization("json");
        dto.setPersistent(false);
        dto.setPersistentDb(null);
        dto.setOrder(true);
        dto.setOrderKey("id");
        dto.setDelayTime(0L);
        dto.setProcessor(null);
        dto.setMessengerGroupId(1L);
        dto.setMhaName("mha1");
        return dto;
    }

    
}
