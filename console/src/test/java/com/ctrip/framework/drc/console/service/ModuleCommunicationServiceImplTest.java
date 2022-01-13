package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.impl.ModuleCommunicationServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.http.HttpUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.web.client.RestClientException;

public class ModuleCommunicationServiceImplTest {

    @InjectMocks
    private ModuleCommunicationServiceImpl moduleCommunicationService;

    @Mock
    private DefaultConsoleConfig config;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void tesGetActiveReplicator() {
        Mockito.doReturn("127.0.0.0:8080").when(config).getCMMetaServerAddress(Mockito.anyString());
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(Mockito.anyString(), Mockito.any(), Mockito.anyString())).thenReturn(new Replicator());            Assert.assertNotNull(moduleCommunicationService.getActiveReplicator("", ""));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(Mockito.anyString(), Mockito.any(), Mockito.anyString())).thenReturn(null);
            Assert.assertNull(moduleCommunicationService.getActiveReplicator("", ""));
        } catch (Exception e) {
            e.printStackTrace();
        }
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(Mockito.anyString(), Mockito.any(), Mockito.anyString())).thenThrow(new RestClientException("http request error"));
            Assert.assertNull(moduleCommunicationService.getActiveReplicator("", ""));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetActiveMySQL() {
        Mockito.doReturn("127.0.0.0:8080").when(config).getCMMetaServerAddress(Mockito.anyString());
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(Mockito.anyString(), DefaultEndPoint.class, Mockito.anyString())).thenReturn(new DefaultEndPoint());
            Assert.assertNotNull(moduleCommunicationService.getActiveReplicator("", ""));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(Mockito.anyString(), DefaultEndPoint.class, Mockito.anyString())).thenReturn(null);
            Assert.assertNull(moduleCommunicationService.getActiveReplicator("", ""));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
