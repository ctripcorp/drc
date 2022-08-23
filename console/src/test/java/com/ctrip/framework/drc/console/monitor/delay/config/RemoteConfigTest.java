package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.http.HttpUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.HashMap;
import java.util.HashSet;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;

public class RemoteConfigTest extends AbstractConfigTest {

    @InjectMocks
    private RemoteConfig remoteConfig = new RemoteConfig();

    @Mock
    private DefaultConsoleConfig consoleConfig = new DefaultConsoleConfig();

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider = new DbClusterSourceProvider();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testUpdateConfig() throws Exception {
        Mockito.when(consoleConfig.getCenterRegionUrl()).thenReturn("http://127.0.0.1:8080");
        Mockito.doReturn("dc").when(dbClusterSourceProvider).getLocalDcName();

        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> HttpUtils.get("http://127.0.0.1:8080/api/drc/v1/meta/", String.class)).thenReturn(DRC_XML);
            remoteConfig.updateConfig();
            Assert.assertNotNull(remoteConfig.xml);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Mockito.doReturn(new HashSet<> () {{add("dc");}}).when(consoleConfig).getLocalConfigCloudDc();
        remoteConfig.updateConfig();
    }
    

}
