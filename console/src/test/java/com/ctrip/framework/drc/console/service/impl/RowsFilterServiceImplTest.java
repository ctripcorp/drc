package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.UdlMigrateConfiguration;
import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.util.List;


public class RowsFilterServiceImplTest extends AbstractTest {

    @Mock
    private DataMediaTblDao dataMediaTblDao;

    @Mock
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;

    @Mock
    private RowsFilterTblDao rowsFilterTblDao;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private UdlMigrateConfiguration udlMigrateConfig;
    
    @InjectMocks
    private RowsFilterServiceImpl rowsFilterService;


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }
    @Test
    public void testGenerateRowsFiltersConfig() throws SQLException {
        // mock
        RowsFilterMappingTbl rowsFilterMappingTbl = new RowsFilterMappingTbl();
        rowsFilterMappingTbl.setApplierGroupId(1L);
        rowsFilterMappingTbl.setRowsFilterId(1L);
        rowsFilterMappingTbl.setDataMediaId(1L);
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = Lists.newArrayList(rowsFilterMappingTbl);
        Mockito.when(rowsFilterMappingTblDao.queryByApplierGroupIds(Mockito.anyList(),Mockito.anyInt())).
                thenReturn(rowsFilterMappingTbls);
        
        
        //mock
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setNamespcae("db[01-32]");
        dataMediaTbl.setName("table1|table2");
        List<DataMediaTbl> dataMediaTbls = Lists.newArrayList(dataMediaTbl);
        Mockito.when(dataMediaTblDao.queryByIdsAndType(
                    Mockito.anyList(),
                    Mockito.eq(DataMediaTypeEnum.ROWS_FILTER.getType()),
                    Mockito.eq(BooleanEnum.FALSE.getCode()))).
                thenReturn(dataMediaTbls);
        
        //mock
        Mockito.when(udlMigrateConfig.gray(Mockito.eq(1L))).thenReturn(true);
        
        //mock
        Mockito.when(rowsFilterTblDao.update(Mockito.any(RowsFilterTbl.class))).thenReturn(1);


        // test 1
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(1L);
        rowsFilterTbl.setMode(RowsFilterType.TripUid.getName());
        rowsFilterTbl.setParameters("{\n" +
                "                    \"columns\": [\n" +
                "                        \"columnA\",\n" +
                "                        \"columnB\",\n" +
                "                        \"cloumnC\"\n" +
                "                    ],\n" +
                "                    \"context\": \"SIN\"\n" +
                "                }");
        Mockito.when(rowsFilterTblDao.queryById(Mockito.eq(Long.valueOf(1L)),Mockito.eq(BooleanEnum.FALSE.getCode()))).
                thenReturn(rowsFilterTbl);
        
        List<RowsFilterConfig> rowsFilterConfigs = rowsFilterService.generateRowsFiltersConfig(1L);
        System.out.println("test1" + JsonUtils.toJson(rowsFilterConfigs.get(0)));
        Assert.assertEquals(1,rowsFilterConfigs.size());
        Assert.assertEquals(RowsFilterType.TripUdl.getName(),rowsFilterConfigs.get(0).getMode());
        
        // test 2
        rowsFilterTbl.setId(1L);
        rowsFilterTbl.setMode(RowsFilterType.TripUdl.getName());
        rowsFilterTbl.setParameters("{\n" +
                "                    \"columns\": [\n" +
                "                        \"columnA\",\n" +
                "                        \"columnB\",\n" +
                "                        \"cloumnC\"\n" +
                "                    ],\n" +
                "                    \"context\": \"SIN\"\n" +
                "                }");
        rowsFilterTbl.setConfigs("{\n" +
                "    \"parameterList\": [\n" +
                "        {\n" +
                "            \"columns\": [\n" +
                "                \"columnB\"\n" +
                "            ],\n" +
                "            \"illegalArgument\": false,\n" +
                "            \"context\": \"context\",\n" +
                "            \"fetchMode\": 0,\n" +
                "            \"userFilterMode\": \"udl\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"columns\": [\n" +
                "                \"columnA\"\n" +
                "            ],\n" +
                "            \"illegalArgument\": false,\n" +
                "            \"context\": \"context\",\n" +
                "            \"fetchMode\": 0,\n" +
                "            \"userFilterMode\": \"uid\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n");
        Mockito.when(rowsFilterTblDao.queryById(Mockito.eq(Long.valueOf(1L)),Mockito.eq(BooleanEnum.FALSE.getCode()))).
                thenReturn(rowsFilterTbl);
        rowsFilterConfigs = rowsFilterService.generateRowsFiltersConfig(1L);
        System.out.println("test2" + JsonUtils.toJson(rowsFilterConfigs.get(0)));
        Assert.assertEquals(1,rowsFilterConfigs.size());
        Assert.assertEquals(2,rowsFilterConfigs.get(0).getConfigs().getParameterList().size());
        Assert.assertEquals(RowsFilterType.TripUdl.getName(),rowsFilterConfigs.get(0).getMode());
        
        //mock
        Mockito.when(udlMigrateConfig.gray(Mockito.eq(1L))).thenReturn(false);
        // test 3
        rowsFilterConfigs = rowsFilterService.generateRowsFiltersConfig(1L);
        System.out.println("test3" +JsonUtils.toJson(rowsFilterConfigs.get(0)));
        Assert.assertEquals(RowsFilterType.TripUid.getName(),rowsFilterConfigs.get(0).getMode());
    }
    
}