package com.ctrip.framework.drc.console.controller.v2;

import com.alibaba.arthas.deps.org.slf4j.Logger;
import com.alibaba.arthas.deps.org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.CollectionUtils;

import java.util.List;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(MockitoJUnitRunner.class)
public class MetaControllerV2Test {
    protected Logger logger = LoggerFactory.getLogger(getClass());


    private MockMvc mvc;

    @InjectMocks
    private MetaControllerV2 controller;

    @Mock
    private MhaServiceV2 mhaServiceV2;

    @Mock
    private MhaReplicationServiceV2 mhaReplicationServiceV2;

    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Mock
    private MetaProviderV2 metaProviderV2;

    @Before
    public void setUp() {
        this.mvc = MockMvcBuilders.standaloneSetup(controller).build();
        List<BuTbl> buTbls = JSON.parseArray("[{\"id\":3,\"buName\":\"BBZ\",\"deleted\":0,\"createTime\":1606209449710,\"datachangeLasttime\":1606209449710},{\"id\":4,\"buName\":\"IBU\",\"deleted\":0,\"createTime\":1606209670944,\"datachangeLasttime\":1606209670944},{\"id\":5,\"buName\":\"FX\",\"deleted\":0,\"createTime\":1618482818623,\"datachangeLasttime\":1618482818623},{\"id\":6,\"buName\":\"MBU\",\"deleted\":0,\"createTime\":1620897009325,\"datachangeLasttime\":1620897009325},{\"id\":7,\"buName\":\"BBZ3\",\"deleted\":0,\"createTime\":1621575183327,\"datachangeLasttime\":1621575183327},{\"id\":8,\"buName\":\"TRN\",\"deleted\":0,\"createTime\":1622514029050,\"datachangeLasttime\":1622514029050},{\"id\":9,\"buName\":\"PUB\",\"deleted\":0,\"createTime\":1645683319914,\"datachangeLasttime\":1645683319914},{\"id\":11,\"buName\":\"BBZTEST\",\"deleted\":0,\"createTime\":1655886120549,\"datachangeLasttime\":1655886120549},{\"id\":12,\"buName\":\"MKT\",\"deleted\":0,\"createTime\":1672817827782,\"datachangeLasttime\":1672817827782},{\"id\":13,\"buName\":\"GS\",\"deleted\":0,\"createTime\":1679900615228,\"datachangeLasttime\":1679900615228},{\"id\":14,\"buName\":\"FLT\",\"deleted\":0,\"createTime\":1679916177111,\"datachangeLasttime\":1679916177111},{\"id\":15,\"buName\":\"TOUR\",\"deleted\":0,\"createTime\":1688623962087,\"datachangeLasttime\":1688623962087}]", BuTbl.class);
        List<DcDo> dcDos = JSON.parseArray("[{\"dcId\":26,\"dcName\":\"shaoy\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":27,\"dcName\":\"sharb\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":30,\"dcName\":\"ntgxh\",\"regionId\":3,\"regionName\":\"ntgxh\"},{\"dcId\":32,\"dcName\":\"ntgxy\",\"regionId\":4,\"regionName\":\"ntgxy\"},{\"dcId\":34,\"dcName\":\"shali\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":35,\"dcName\":\"shaxy\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":36,\"dcName\":\"sinaws\",\"regionId\":2,\"regionName\":\"sin\"},{\"dcId\":37,\"dcName\":\"fraaws\",\"regionId\":5,\"regionName\":\"fra\"}]", DcDo.class);
        List<RegionTbl> regionTbls = JSON.parseArray("[{\"id\":1,\"regionName\":\"sha\",\"deleted\":0,\"createTime\":1687763702342,\"datachangeLasttime\":1687763702342},{\"id\":2,\"regionName\":\"sin\",\"deleted\":0,\"createTime\":1687763702342,\"datachangeLasttime\":1687763702342},{\"id\":3,\"regionName\":\"ntgxh\",\"deleted\":0,\"createTime\":1687763702342,\"datachangeLasttime\":1687763702342},{\"id\":4,\"regionName\":\"ntgxy\",\"deleted\":0,\"createTime\":1687763702342,\"datachangeLasttime\":1687763702342},{\"id\":5,\"regionName\":\"fra\",\"deleted\":0,\"createTime\":1687763783347,\"datachangeLasttime\":1687763783347}]", RegionTbl.class);
        Mockito.when(metaInfoServiceV2.queryAllBuWithCache()).thenReturn(buTbls);
        Mockito.when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(dcDos);
        Mockito.when(metaInfoServiceV2.queryAllRegionWithCache()).thenReturn(regionTbls);
    }

    @Test
    public void getAllBuTbls() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/meta/bus/all")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);

        Assert.assertTrue(!CollectionUtils.isEmpty(apiResult.getData()));
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), apiResult.getStatus().longValue());
    }

    @Test
    public void getAllRegionTbls() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/meta/regions/all")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);

        Assert.assertTrue(!CollectionUtils.isEmpty(apiResult.getData()));
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), apiResult.getStatus().longValue());

    }

    @Test
    public void getAllDcs() throws Exception {
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/meta/dcs/all")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);

        Assert.assertTrue(!CollectionUtils.isEmpty(apiResult.getData()));
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), apiResult.getStatus().longValue());
    }


    @Test
    public void getAllDcsException() throws Exception {
        Mockito.when(metaInfoServiceV2.queryAllDcWithCache()).thenThrow(ConsoleException.class);
        MvcResult relatedMhaId = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/meta/dcs/all")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        ApiResult<List> apiResult = JSON.parseObject(relatedMhaId.getResponse().getContentAsString(), ApiResult.class);

        Assert.assertTrue(CollectionUtils.isEmpty(apiResult.getData()));
        Assert.assertEquals(ResultCode.HANDLE_FAIL.getCode(), apiResult.getStatus().longValue());
    }

    @Test
    public void testQueryMhaReplicationDetailConfig() throws Exception {
        when(metaInfoServiceV2.getDrcReplicationConfig(anyLong())).thenReturn(new Drc());

        ApiResult<String> result = controller.queryMhaReplicationDetailConfig(Long.valueOf(1));
        verify(metaInfoServiceV2, times(1)).getDrcReplicationConfig(anyLong());
    }

    @Test
    public void testQueryMhaReplicationDetailConfigByName() throws Exception {
        when(metaInfoServiceV2.getDrcReplicationConfig(anyString(), anyString())).thenReturn(new Drc());

        ApiResult<String> result = controller.queryMhaReplicationDetailConfig("mha1", "mha2");
        verify(metaInfoServiceV2, times(1)).getDrcReplicationConfig(anyString(),anyString());
    }

    @Test
    public void testQueryMhaMessengerDetailConfig() throws Exception {
        when(metaInfoServiceV2.getDrcMessengerConfig(anyString())).thenReturn(new Drc());

        ApiResult<String> result = controller.queryMhaMessengerDetailConfig("mhaName");
        verify(metaInfoServiceV2, times(1)).getDrcMessengerConfig(anyString());
    }

    @Test
    public void testGetAllMetaData() throws Exception {
        Mockito.when(metaProviderV2.getDrc()).thenThrow(ConsoleException.class);
        String result = controller.getAllMetaData("false");
        Assert.assertNull(result);
    }
}