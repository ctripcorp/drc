package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.service.v2.DataMediaServiceV2;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.mq.MqType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.SQLException;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.when;

public class MetaInfoServiceV2ImplTest2 extends CommonDataInit {

    public static String PROXY = "PROXY";
    public static String IP_DC1_1 = "10.25.222.15";
    public static String PORT_IN = "80";

    public static String PROXY_DC1_1 = String.format("%s://%s:%s", PROXY, IP_DC1_1, PORT_IN);
    MqType mqType = MqType.qmq;

    @Mock
    DataMediaServiceV2 dataMediaService;

    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
        when(dataMediaService.generateConfigFast(anyList())).thenReturn(new DataMediaConfig());
    }

    @Test
    public void testGetDrcMessengerConfig() throws Exception {
        Drc result = metaInfoServiceV2Impl.getDrcMessengerConfig("mha1", mqType);
    }

}
