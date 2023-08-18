package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

import static org.mockito.Mockito.*;

public class MessengerControllerV2Test {
    @Mock
    MessengerServiceV2 messengerService;
    @Mock
    MetaInfoServiceV2 metaInfoServiceV2;
    @InjectMocks
    MessengerControllerV2 messengerControllerV2;
    @Mock
    DefaultConsoleConfig defaultConsoleConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(defaultConsoleConfig.getNewDrcConfigSwitch()).thenReturn(DefaultConsoleConfig.SWITCH_ON);
    }

    @Test
    public void testGetAllMessengerVos() throws Exception {
        long buId = 1L;
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setBuId(buId);
        mhaTblV2.setMonitorSwitch(0);
        when(messengerService.getAllMessengerMhaTbls()).thenReturn(List.of(mhaTblV2));
        BuTbl e1 = new BuTbl();
        e1.setId(buId);
        e1.setBuName("bbz");
        when(metaInfoServiceV2.queryAllBuWithCache()).thenReturn(List.of(e1));

        ApiResult<List<MessengerVo>> result = messengerControllerV2.getAllMessengerVos();
        MessengerVo data = result.getData().get(0);
        Assert.assertEquals(data.getMhaName(), mhaTblV2.getMhaName());
        Assert.assertEquals(data.getBu(), e1.getBuName());
    }

    @Test
    public void testGetAllMqConfigsByMhaName() throws Exception {
        List<MqConfigVo> value = List.of(new MqConfigVo());
        when(messengerService.queryMhaMessengerConfigs(anyString())).thenReturn(value);

        ApiResult<List<MqConfigVo>> result = messengerControllerV2.getAllMqConfigsByMhaName("mhaName");
        Assert.assertEquals(value, result.getData());
    }

    @Test
    public void testGetBuListFromQmq() throws Exception {
        List<String> bus = List.of("String");
        when(messengerService.getBusFromQmq()).thenReturn(bus);

        ApiResult<List<String>> result = messengerControllerV2.getBuListFromQmq();
        Assert.assertEquals(bus, result.getData());
    }

    @Test
    public void testCheckMqConfig() throws Exception {
        MqConfigCheckVo res = new MqConfigCheckVo();
        res.setAllowSubmit(true);
        res.setConflictTables(null);
        when(messengerService.checkMqConfig(any())).thenReturn(res);

        MqConfigDto dto = new MqConfigDto();
        dto.setMhaName("testMha");
        dto.setTable("testDb\\.(testTable)");
        ApiResult<MqConfigCheckVo> result = messengerControllerV2.checkMqConfig(dto);
        Assert.assertEquals(res, result.getData());
    }

    @Test
    public void testAddConfig() throws Exception {
        MqConfigDto dto = new MqConfigDto();

        ApiResult<Boolean> result = messengerControllerV2.submitConfig(dto);
        verify(messengerService, times(1)).processAddMqConfig(dto);
    }

    @Test
    public void testSubmitConfig() throws Exception {
        MqConfigDto dto = new MqConfigDto();
        dto.setDbReplicationId(1L);

        ApiResult<Boolean> result = messengerControllerV2.submitConfig(dto);
        verify(messengerService, times(1)).processUpdateMqConfig(dto);
    }

    @Test
    public void testDeleteMqConfig() throws Exception {
        MqConfigDeleteRequestDto requestDto = new MqConfigDeleteRequestDto();
        requestDto.setMhaName("testMha");
        requestDto.setDbReplicationIdList(Lists.newArrayList(1L));
        ApiResult<Void> result = messengerControllerV2.deleteMqConfig(requestDto);
        verify(messengerService, times(1)).processDeleteMqConfig(any());
    }

    @Test
    public void testGetMessengerExecutedGtid() throws Exception {
        String gtid = "getMessengerGtidExecutedResponse";
        when(messengerService.getMessengerGtidExecuted(anyString())).thenReturn(gtid);

        ApiResult result = messengerControllerV2.getMessengerExecutedGtid("mhaName");
        Assert.assertEquals(gtid, result.getData());
    }
}
