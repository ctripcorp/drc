package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.service.filter.impl.RowsFilterMetaServiceImpl;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;
import com.ctrip.framework.drc.console.vo.filter.QConfigDetailData;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.ArrayList;

/**
 * Created by dengquanliang
 * 2023/5/4 16:47
 */
public class RowsFilterMetaServiceTest {

    @InjectMocks
    private RowsFilterMetaServiceImpl rowsFilterMetaService;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private QConfigApiService qConfigApiService;
    @Mock
    private RowsFilterMetaMappingTblDao rowsFilterMetaMappingTblDao;
    @Mock
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;
    @Mock
    private EventMonitor eventMonitor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.doNothing().when(eventMonitor).logEvent(Mockito.anyString(), Mockito.anyString());
        Mockito.when(domainConfig.getQConfigApiConsoleToken()).thenReturn("token");
        Mockito.when(domainConfig.getWhiteListTargetSubEnv()).thenReturn(new ArrayList<>());
        Mockito.when(domainConfig.getWhitelistTargetGroupId()).thenReturn("targetGroupId");

        Mockito.when(rowsFilterMetaTblDao.queryOneByMetaFilterName(Mockito.anyString())).thenReturn(buildRowsFilterMetaTbl());
        Mockito.when(rowsFilterMetaMappingTblDao.queryByMetaFilterId(Mockito.anyLong())).thenReturn(Lists.newArrayList(new RowsFilterMetaMappingTbl()));
        Mockito.when(qConfigApiService.getQConfigData(Mockito.any(QConfigQueryParam.class))).thenReturn(buildExistQConfigResponse());

    }

    @Test
    public void testGetWhitelist() throws Exception {
        try (MockedStatic<JsonUtils> theMock = Mockito.mockStatic(JsonUtils.class)) {
            theMock.when(() -> {
                JsonUtils.fromJsonToList(Mockito.anyString(), Mockito.eq(String.class));
            }).thenReturn(Lists.newArrayList("sub"));

            QConfigDataVO result = rowsFilterMetaService.getWhitelist("metaFilterName");
            Mockito.verify(qConfigApiService, Mockito.times(1)).getQConfigData(Mockito.any(QConfigQueryParam.class));
            Assert.assertNotNull(result);
        }

    }

    @Test
    public void testAddWhitelist() throws Exception {
        try (MockedStatic<JsonUtils> theMock = Mockito.mockStatic(JsonUtils.class)) {
            theMock.when(() -> {
                JsonUtils.fromJsonToList(Mockito.anyString(), Mockito.eq(String.class));
            }).thenReturn(Lists.newArrayList("sub"));

            Mockito.when(qConfigApiService.batchUpdateConfig(Mockito.any(QConfigBatchUpdateParam.class))).thenReturn(getValidResponse());
            boolean result = rowsFilterMetaService.addWhitelist(buildRowsMetaFilterParam(), "operator");
            Mockito.verify(qConfigApiService, Mockito.times(1)).batchUpdateConfig(Mockito.any(QConfigBatchUpdateParam.class));
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testDeleteWhitelist() throws Exception {
        try (MockedStatic<JsonUtils> theMock = Mockito.mockStatic(JsonUtils.class)) {
            theMock.when(() -> {
                JsonUtils.fromJsonToList(Mockito.anyString(), Mockito.eq(String.class));
            }).thenReturn(Lists.newArrayList("sub"));

            Mockito.when(qConfigApiService.batchUpdateConfig(Mockito.any(QConfigBatchUpdateParam.class))).thenReturn(getValidResponse());
            boolean result = rowsFilterMetaService.deleteWhitelist(buildRowsMetaFilterParam(), "operator");
            Mockito.verify(qConfigApiService, Mockito.times(1)).batchUpdateConfig(Mockito.any(QConfigBatchUpdateParam.class));
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testUpdateWhitelist() throws Exception {
        try (MockedStatic<JsonUtils> theMock = Mockito.mockStatic(JsonUtils.class)) {
            theMock.when(() -> {
                JsonUtils.fromJsonToList(Mockito.anyString(), Mockito.eq(String.class));
            }).thenReturn(Lists.newArrayList("sub"));

            Mockito.when(qConfigApiService.batchUpdateConfig(Mockito.any(QConfigBatchUpdateParam.class))).thenReturn(getInValidResponse());
            boolean result = rowsFilterMetaService.updateWhitelist(buildRowsMetaFilterParam(), "operator");
            Mockito.verify(qConfigApiService, Mockito.times(3)).batchUpdateConfig(Mockito.any(QConfigBatchUpdateParam.class));
            Assert.assertFalse(result);
        }
    }

    private RowsFilterMetaTbl buildRowsFilterMetaTbl() {
        RowsFilterMetaTbl rowsFilterMetaTbl = new RowsFilterMetaTbl();
        rowsFilterMetaTbl.setId(0L);
        rowsFilterMetaTbl.setTargetSubenv("");
        return rowsFilterMetaTbl;
    }

    private RowsMetaFilterParam buildRowsMetaFilterParam() {
        RowsMetaFilterParam param = new RowsMetaFilterParam();
        param.setMetaFilterName("metaFilterName");
        param.setWhitelist(Lists.newArrayList("val1", "val2"));
        return param;
    }

    private QConfigDataResponse buildExistQConfigResponse() {
        QConfigDataResponse res = new QConfigDataResponse();
        res.setStatus(0);
        res.setData(new QConfigDetailData());
        return res;
    }

    private UpdateQConfigResponse getValidResponse() {
        UpdateQConfigResponse response = new UpdateQConfigResponse();
        response.setStatus(0);
        return response;
    }

    private UpdateQConfigResponse getInValidResponse() {
        UpdateQConfigResponse response = new UpdateQConfigResponse();
        response.setStatus(-1);
        return response;
    }

}
