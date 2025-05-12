package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.v3.MqAutoCreateRequestDto;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.vo.v2.MqMetaCreateResultView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by shiruixin
 * 2025/5/12 17:49
 */
public class DbDrcBuildControllerV2Test {
    @InjectMocks
    private DbDrcBuildControllerV2 dbDrcBuildControllerV2;
    @Mock
    DefaultConsoleConfig defaultConsoleConfig;
    @Mock
    private DbDrcBuildService dbDrcBuildService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

    }

    @Test
    public void testAutoCreateMq() {
        Mockito.when(dbDrcBuildService.autoCreateMq(Mockito.any())).thenReturn(new MqMetaCreateResultView("error"));
        MqAutoCreateRequestDto dto = new MqAutoCreateRequestDto();
        dto.setDbName("db");
        dto.setMqType("qmq");
        dto.setTable("table");
        dto.setTopic("bu.topic");
        dto.setBu("bu");
        dto.setRegion("region");
        ApiResult<MqMetaCreateResultView> res = dbDrcBuildControllerV2.autoCreateMq(dto);
        Assert.assertEquals(Integer.valueOf(1), res.getStatus());

        MqMetaCreateResultView view = new MqMetaCreateResultView();
        view.setContainTables(0);
        Mockito.when(dbDrcBuildService.autoCreateMq(Mockito.any())).thenReturn(view);
        res = dbDrcBuildControllerV2.autoCreateMq(dto);
        Assert.assertEquals(Integer.valueOf(0), res.getStatus());

    }

    @Test
    public void testAutoCreateMqForward() {
        MqAutoCreateRequestDto dto = new MqAutoCreateRequestDto();
        dto.setDbName("db");
        dto.setMqType("qmq");
        dto.setTable("table");
        dto.setTopic("bu.topic");
        dto.setBu("bu");
        dto.setRegion("region");
        dbDrcBuildControllerV2.autoCreateMqForward(dto);
    }
}