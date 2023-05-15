package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.QConfigRevertParam;
import com.ctrip.framework.drc.console.service.filter.impl.QConfigApiServiceImpl;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/5/15 16:40
 */
public class QConfigApiServiceTest {

    @InjectMocks
    private QConfigApiServiceImpl qConfigApiService;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private EventMonitor eventMonitor;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.doNothing().when(eventMonitor).logEvent(Mockito.anyString(), Mockito.anyString());
        Mockito.when(domainConfig.getQConfigRestApiUrl()).thenReturn("url");
    }

    @Test
    public void testGetQConfigData() {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.get(Mockito.anyString(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(buildExistDataResponse());

            QConfigQueryParam param = new QConfigQueryParam();
            param.setSubEnv("subEnv");
            QConfigDataResponse response = qConfigApiService.getQConfigData(param);
            Assert.assertNotNull(response);
        }
    }

    @Test
    public void testBatchUpdateConfig() {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(new UpdateQConfigResponse());

            QConfigBatchUpdateParam param = new QConfigBatchUpdateParam();
            param.setTargetSubEnv("subEnv");
            UpdateQConfigResponse response = qConfigApiService.batchUpdateConfig(param);
            Assert.assertNotNull(response);
        }
    }

    @Test
    public void testRevertConfig() {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(new UpdateQConfigResponse());

            QConfigRevertParam param = new QConfigRevertParam();
            param.setTargetSubEnv("subEnv");
            UpdateQConfigResponse response = qConfigApiService.revertConfig(param);
            Assert.assertNotNull(response);
        }
    }

    private QConfigDataResponse buildExistDataResponse() {
        QConfigDataResponse response = new QConfigDataResponse();
        response.setStatus(0);
        return response;
    }

}
