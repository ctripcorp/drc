package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MessengerQueryDto;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.mq.MqType;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

    MqType mqType = MqType.DEFAULT;
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(defaultConsoleConfig.getNewDrcConfigSwitch()).thenReturn(DefaultConsoleConfig.SWITCH_ON);
    }


    @Test
    public void testQueryAllMessengerVos() throws Exception {
        MessengerVo messengerVo = new MessengerVo();
        messengerVo.setBu("bbz");
        messengerVo.setMhaName(null);
        when(messengerService.getMessengerMhaTbls(any())).thenReturn(List.of(messengerVo));

        MessengerQueryDto queryDto = new MessengerQueryDto();
        queryDto.setMqType(MqType.DEFAULT.name());
        ApiResult<List<MessengerVo>> result = messengerControllerV2.queryMessengerVos(queryDto);
        MessengerVo data = result.getData().get(0);
        Assert.assertEquals(data.getMhaName(), messengerVo.getMhaName());
        Assert.assertEquals(data.getBu(), messengerVo.getBu());
    }

    @Test
    public void testGetAllMqConfigsByMhaName() throws Exception {
        List<MqConfigVo> value = List.of(new MqConfigVo());
        when(messengerService.queryMhaMessengerConfigs(anyString(), eq(MqType.DEFAULT))).thenReturn(value);

        ApiResult<List<MqConfigVo>> result = messengerControllerV2.getAllMqConfigsByMhaName("mhaName", MqType.qmq.name());
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
        dto.setMqType(MqType.DEFAULT.name());
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
        requestDto.setMqType(MqType.DEFAULT.name());
        ApiResult<Void> result = messengerControllerV2.deleteMqConfig(requestDto);
        verify(messengerService, times(1)).processDeleteMqConfig(any());
    }

    @Test
    public void testDeleteMha() {
        ApiResult<Boolean> result = messengerControllerV2.removeMessengerGroupInMha("mhaName", mqType.name());
        Assert.assertEquals((Integer) ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus());
    }

}
