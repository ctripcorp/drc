package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;

public class RemoteConfigTest extends AbstractConfigTest {

    @InjectMocks
    private RemoteConfig remoteConfig = new RemoteConfig();

    @Mock
    private DefaultConsoleConfig consoleConfig = new DefaultConsoleConfig();

    @Mock
    private DataCenterService dataCenterService = new DataCenterService();
    
    @Mock
    private HeraldService heraldService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        Mockito.when(consoleConfig.requestWithHeraldToken()).thenReturn(true);
        Mockito.when(consoleConfig.getMetaRealtimeSwitch()).thenReturn("on");
        Mockito.when(heraldService.getLocalHeraldToken()).thenReturn("mockedToken");
    }

    @Test
    public void testUpdateConfig() throws Exception {
        Mockito.when(consoleConfig.getCenterRegionUrl()).thenReturn("http://127.0.0.1:8080");
        Mockito.doReturn("dc").when(dataCenterService).getDc();

        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> HttpUtils.getAcceptAllEncoding("http://127.0.0.1:8080/api/drc/v2/meta/?refresh=true&heraldToken=mockedToken", String.class)).thenReturn(DRC_XML);
            remoteConfig.updateConfig();
            Assert.assertNotNull(remoteConfig.xml);
        } catch (Exception e) {
            e.printStackTrace();
        }

        remoteConfig.updateConfig();
    }
    

}
