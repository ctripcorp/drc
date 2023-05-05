package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.service.filter.impl.RowsFilterMetaMappingServiceImpl;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/5 10:51
 */
public class RowsFilterMetaMappingServiceTest {
    @InjectMocks
    private RowsFilterMetaMappingServiceImpl rowsFilterMetaMappingService;
    @Mock
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;
    @Mock
    private RowsFilterMetaMappingTblDao rowsFilterMetaMappingTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateMetaMessage() throws SQLException {
        Mockito.when(rowsFilterMetaTblDao.insert(Mockito.any(RowsFilterMetaTbl.class))).thenReturn(1);
        boolean result = rowsFilterMetaMappingService.createMetaMessage(buildMessageCreateParam());
        Assert.assertTrue(result);
    }

    @Test
    public void testCreateMetaMapping() throws SQLException {
        Mockito.when(rowsFilterMetaTblDao.queryById(Mockito.anyLong())).thenReturn(new RowsFilterMetaTbl());
        boolean result = rowsFilterMetaMappingService.createMetaMapping(buildMappingCreateParam());
        Assert.assertTrue(result);
    }

    @Test
    public void getMetaMappings() throws SQLException {
        Mockito.when(rowsFilterMetaTblDao.queryByMetaFilterNames(Mockito.anyString())).thenReturn(buildValidMetaTbls());
        Mockito.when(rowsFilterMetaMappingTblDao.queryByMetaFilterIdS(Mockito.anyList())).thenReturn(buildValidMappingTbls());
        List<RowsFilterMetaMappingVO> result = rowsFilterMetaMappingService.getMetaMappings("metaFilterName");
        Assert.assertTrue(result.size() > 0);
    }

    private RowsFilterMetaMessageCreateParam buildMessageCreateParam() {
        RowsFilterMetaMessageCreateParam param = new RowsFilterMetaMessageCreateParam();
        param.setClusterName("clusterName");
        param.setMetaFilterName("metaFilterName");
        param.setTargetSubEnv(Lists.newArrayList("subEnv"));
        param.setBu("bu");
        param.setOwner("owner");
        param.setFilterType(1);
        return param;
    }

    private RowsFilterMetaMappingCreateParam buildMappingCreateParam() {
        RowsFilterMetaMappingCreateParam param = new RowsFilterMetaMappingCreateParam();
        param.setMetaFilterId(1L);
        param.setFilterKeys(Lists.newArrayList("key1", "key2"));
        return param;
    }

    private List<RowsFilterMetaMappingTbl> buildValidMappingTbls() {
        RowsFilterMetaMappingTbl mappingTbl = new RowsFilterMetaMappingTbl();
        mappingTbl.setMetaFilterId(1L);
        mappingTbl.setFilterKey("key");
        return Lists.newArrayList(mappingTbl);
    }

    private List<RowsFilterMetaTbl> buildValidMetaTbls() {
        RowsFilterMetaTbl metaTbl = new RowsFilterMetaTbl();
        metaTbl.setId(0L);
        metaTbl.setTargetSubenv(JsonUtils.toJson(Lists.newArrayList("subEnv")));
        metaTbl.setFilterType(1);
        return Lists.newArrayList(metaTbl);
    }

}
