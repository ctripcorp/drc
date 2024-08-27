package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.v2.DbReplicationService;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.display.v2.DbReplicationVo;
import com.ctrip.framework.drc.console.vo.request.MqReplicationQueryDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Created by shiruixin
 * 2024/8/27 11:45
 */
public class DbReplicationControllerTest {
    @Mock
    DbReplicationService dbReplicationService;
    @InjectMocks
    DbReplicationController dbReplicationController;
    @Mock
    DefaultConsoleConfig defaultConsoleConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(defaultConsoleConfig.getNewDrcConfigSwitch()).thenReturn(DefaultConsoleConfig.SWITCH_ON);
    }

    @Test
    public void testQueryMqReplicationsByPage() throws Exception{
        List<DbReplicationVo> vos = new ArrayList<>();
        vos.add(new DbReplicationVo());
        when(dbReplicationService.queryMqReplicationsByPage(any())).thenReturn(PageResult.newInstance(vos,1,20,1));
        ApiResult<PageResult<DbReplicationVo>> result = dbReplicationController.queryMqReplicationsByPage(new MqReplicationQueryDto());
        Assert.assertNotNull(result);
    }
}
