package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.dao.v2.RowsFilterTblV2Dao;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.console.service.v2.impl.RowsFilterServiceV2Impl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/30 19:25
 */
public class RowsFilterServiceV2Test {

    @InjectMocks
    private RowsFilterServiceV2Impl rowsFilterService;
    @Mock
    private RowsFilterTblV2Dao rowsFilterTblDao;
    
    String udlConfigsString = "{\"parameterList\":[{\"columns\":[\"UID\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}";
    String udlThenUidConfigsString = "{\"parameterList\":[{\"columns\":[\"userdata_location\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"udl\"},{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":2000000002,\"routeStrategyId\":0}";
    String javaRegexConfigsString = "{\"parameterList\":[{\"columns\":[\"salesite\"],\"illegalArgument\":false,\"context\":\"^Trip$\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}";
    List<RowsFilterTblV2> allKindsOfRowsFilters = new ArrayList<>();
    List<RowsFilterTblV2> udlRelatedRowsFilters = new ArrayList<>();

    {
        RowsFilterTblV2 udlRowsFilter = new RowsFilterTblV2();
        udlRowsFilter.setId(1L);
        udlRowsFilter.setConfigs(udlConfigsString);
        udlRowsFilter.setMode(RowsFilterModeEnum.TRIP_UDL.getCode());
        allKindsOfRowsFilters.add(udlRowsFilter);
        udlRelatedRowsFilters.add(udlRowsFilter);

        RowsFilterTblV2 udludiRowsFilter = new RowsFilterTblV2();
        udludiRowsFilter.setId(2L);
        udludiRowsFilter.setConfigs(udlThenUidConfigsString);
        udlRowsFilter.setMode(RowsFilterModeEnum.TRIP_UDL_UID.getCode());
        allKindsOfRowsFilters.add(udludiRowsFilter);
        udlRelatedRowsFilters.add(udludiRowsFilter);

        RowsFilterTblV2 javaRegexRowsFilter = new RowsFilterTblV2();
        javaRegexRowsFilter.setId(3L);
        javaRegexRowsFilter.setConfigs(javaRegexConfigsString);
        javaRegexRowsFilter.setMode(RowsFilterModeEnum.JAVA_REGEX.getCode());
        allKindsOfRowsFilters.add(javaRegexRowsFilter);
    }
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGenerateConfig() throws SQLException {
        Mockito.when(rowsFilterTblDao.queryByIds(Mockito.anyList())).thenReturn(buildRowsFilterTbls());
        List<RowsFilterConfig> result = rowsFilterService.generateRowsFiltersConfig("table", new ArrayList<>());
        Assert.assertEquals(result.size(), 1);
    }

    private List<RowsFilterTblV2> buildRowsFilterTbls() {
        List<RowsFilterTblV2> rowsFilterTbls = new ArrayList<>();
        RowsFilterTblV2 rowsFilterTbl = new RowsFilterTblV2();
        rowsFilterTbl.setConfigs(JsonUtils.toJson(new RowsFilterConfig.Configs()));
        rowsFilterTbl.setMode(RowsFilterModeEnum.JAVA_REGEX.getCode());

        rowsFilterTbls.add(rowsFilterTbl);
        return rowsFilterTbls;
    }

    @Test
    public void testQueryRowsFilterIdsShouldMigrateToSGP() throws SQLException {
        Mockito.when(rowsFilterTblDao.queryByModes(Mockito.anyList())).thenReturn(allKindsOfRowsFilters);
        Assert.assertEquals(2, rowsFilterService.queryRowsFilterIdsShouldMigrate("SIN").size());

        Mockito.when(rowsFilterTblDao.queryByModes(Mockito.anyList())).thenReturn(udlRelatedRowsFilters);
        Assert.assertEquals(2, rowsFilterService.queryRowsFilterIdsShouldMigrate("SIN").size());
    }

    @Test
    public void testMigrateRowsFilterToSGP() throws SQLException {
        List<Long> idsToUpdate = Lists.newArrayList(1L,2L);
        
        Mockito.when(rowsFilterTblDao.queryByModes(Mockito.anyList())).thenReturn(Lists.newArrayList());
        Pair<Boolean, Integer> res = rowsFilterService.migrateRowsFilterUDLRegion(idsToUpdate,"SIN","SGP");
        Assert.assertEquals(false, res.getLeft());
        
        Mockito.when(rowsFilterTblDao.queryByModes(Mockito.anyList())).thenReturn(udlRelatedRowsFilters);
        Mockito.when(rowsFilterTblDao.queryByIds(Mockito.anyList())).thenReturn(udlRelatedRowsFilters);
        Mockito.when(rowsFilterTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[]{1,1});
        res = rowsFilterService.migrateRowsFilterUDLRegion(idsToUpdate,"SIN","SGP");
        Assert.assertEquals(true, res.getLeft());
        Assert.assertEquals(2, res.getRight().intValue());

        idsToUpdate.add(3L);
        res = rowsFilterService.migrateRowsFilterUDLRegion(idsToUpdate,"SIN","SGP");
        Assert.assertEquals(false, res.getLeft());
        
    }
    
}
