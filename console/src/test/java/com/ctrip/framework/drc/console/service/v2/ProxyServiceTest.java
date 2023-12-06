package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ProxyTblDao;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.service.v2.resource.impl.ProxyServiceImpl;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getDcTbls;
import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getProxyTbls;

/**
 * Created by dengquanliang
 * 2023/12/6 21:12
 */
public class ProxyServiceTest {

    @InjectMocks
    private ProxyServiceImpl proxyService;

    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private BuTblDao buTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetProxyUris() throws Exception {
        Mockito.when(consoleConfig.getDcsInSameRegion(Mockito.anyString())).thenReturn(Sets.newHashSet("dc"));
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(proxyTblDao.queryByDcId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getProxyTbls());

        List<String> result = proxyService.getProxyUris("dc");
        Assert.assertEquals(result.size(), getProxyTbls().size());
    }

    @Test
    public void testGetAllProxyUris() throws Exception {
        Mockito.when(proxyTblDao.queryAllExist()).thenReturn(getProxyTbls());
        List<String> result = proxyService.getAllProxyUris();
        Assert.assertEquals(result.size(), getProxyTbls().size());
    }

    @Test
    public void testInputProxy() throws SQLException {
        Mockito.doNothing().when(proxyTblDao).upsert(Mockito.any(ProxyTbl.class));
        Mockito.when(dcTblDao.upsert(Mockito.anyString())).thenReturn(1L);

        ProxyDto proxyDto = new ProxyDto("dc", "protocol", "ip", "port");
        proxyService.inputProxy(proxyDto);
        Mockito.verify(proxyTblDao, Mockito.times(1)).upsert(Mockito.any(ProxyTbl.class));
    }
}
