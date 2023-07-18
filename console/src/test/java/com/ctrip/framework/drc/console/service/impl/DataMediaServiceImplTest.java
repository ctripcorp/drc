package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.service.ColumnsFilterService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterMode;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

public class DataMediaServiceImplTest {

    @InjectMocks
    private DataMediaServiceImpl dataMediaService;

    @Mock
    private DataMediaTblDao dataMediaTblDao;

    @Mock
    private ColumnsFilterService columnsFilterService;

    @Mock
    private RowsFilterService rowsFilterService;

    @Mock
    private ApplierGroupTblDao applierGroupTblDao;

    @Mock
    private DrcDoubleWriteService drcDoubleWriteService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(1L);
        dataMediaTbl.setNamespcae("db1");
        dataMediaTbl.setName("table1");
        dataMediaTbl.setType(0);
        Mockito.when(dataMediaTblDao.queryByAGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(
                Lists.newArrayList(dataMediaTbl));

        ColumnsFilterConfig columnsFilterConfig = new ColumnsFilterConfig();
        columnsFilterConfig.setTables(dataMediaTbl.getFullName());
        columnsFilterConfig.setMode("exclude");
        columnsFilterConfig.setColumns(Lists.newArrayList("column1"));
        Mockito.when(columnsFilterService.generateColumnsFilterConfig(Mockito.any(DataMediaTbl.class))).
                thenReturn(columnsFilterConfig);

        RowsFilterConfig rowsFilterConfig = new RowsFilterConfig();
        rowsFilterConfig.setMode("trip_udl");
        rowsFilterConfig.setTables("db1.table1");
        Mockito.when(rowsFilterService.generateRowsFiltersConfig(Mockito.anyLong(), Mockito.anyInt())).
                thenReturn(Lists.newArrayList(rowsFilterConfig));

    }

    @Test
    public void testGenerateConfig() throws SQLException {
        DataMediaConfig dataMediaConfig = dataMediaService.generateConfig(1L);
        List<ColumnsFilterConfig> columnsFilters = dataMediaConfig.getColumnsFilters();
        Assert.assertEquals(1, columnsFilters.size());
        Assert.assertEquals(ColumnsFilterMode.EXCLUDE, columnsFilters.get(0).getColumnsFilterMode());
    }

    @Test
    public void testProcessAddDataMedia() throws SQLException {
        Mockito.when(dataMediaTblDao.insertReturnPk(Mockito.any(DataMediaTbl.class))).thenReturn(1L);
        Mockito.when(applierGroupTblDao.queryByPk(Mockito.anyLong())).thenReturn(buildApplierGroupTbl());
        DataMediaDto dto = new DataMediaDto();
        dto.setId(0L);
        dto.setNamespace("db1");
        dto.setName("table1");
        dto.setApplierGroupId(1L);
        dto.setType(0);
        dto.setDataMediaSourceId(1L);
        Long dataMediaId = dataMediaService.processAddDataMedia(dto);
        Assert.assertEquals(1L, dataMediaId.longValue());
    }

    @Test
    public void testProcessUpdateDataMedia() throws SQLException {
        Mockito.when(dataMediaTblDao.update(Mockito.any(DataMediaTbl.class))).thenReturn(1);
        Mockito.when(applierGroupTblDao.queryByPk(Mockito.anyLong())).thenReturn(buildApplierGroupTbl());
        DataMediaDto dto = new DataMediaDto();
        dto.setId(1L);
        dto.setNamespace("db1");
        dto.setName("table1");
        dto.setApplierGroupId(1L);
        dto.setType(0);
        dto.setDataMediaSourceId(1L);
        Long dataMediaId = dataMediaService.processUpdateDataMedia(dto);
        Assert.assertEquals(1L, dataMediaId.longValue());
    }

    @Test
    public void testProcessDeleteDataMedia() throws Exception {
        Mockito.when(dataMediaTblDao.update(Mockito.any(DataMediaTbl.class))).thenReturn(1);
        Mockito.doNothing().when(drcDoubleWriteService).deleteColumnsFilter(Mockito.anyLong());
        Long dataMediaId = dataMediaService.processDeleteDataMedia(1L);
        Assert.assertEquals(1L, dataMediaId.longValue());
    }

    @Test
    public void testProcessAddColumnsFilterConfig() throws Exception {
        Mockito.when(columnsFilterService.addColumnsFilterConfig(Mockito.any(ColumnsFilterConfigDto.class))).
                thenReturn("add ColumnsFilterConfig success");
        Mockito.doNothing().when(drcDoubleWriteService).insertColumnsFilter(Mockito.anyLong());
        dataMediaService.processAddColumnsFilterConfig(new ColumnsFilterConfigDto());
    }

    @Test
    public void testProcessUpdateColumnsFilterConfig() throws Exception {
        Mockito.when(columnsFilterService.updateColumnsFilterConfig(Mockito.any(ColumnsFilterConfigDto.class))).
                thenReturn("update ColumnsFilterConfig success");
        Mockito.doNothing().when(drcDoubleWriteService).insertColumnsFilter(Mockito.anyLong());
        Mockito.doNothing().when(drcDoubleWriteService).deleteColumnsFilter(Mockito.anyLong());
        dataMediaService.processUpdateColumnsFilterConfig(new ColumnsFilterConfigDto());
    }

    @Test
    public void testProcessDeleteColumnsFilterConfig() throws SQLException {
        Mockito.when(columnsFilterService.deleteColumnsFilterConfig(Mockito.anyLong())).
                thenReturn("delete ColumnsFilterConfig success");
        dataMediaService.processDeleteColumnsFilterConfig(1L);
    }

    private ApplierGroupTbl buildApplierGroupTbl() {
        ApplierGroupTbl tbl = new ApplierGroupTbl();
        tbl.setNameFilter("db1\\.table1");

        return tbl;
    }

}