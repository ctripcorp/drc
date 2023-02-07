package com.ctrip.framework.drc.console.service.impl;



import com.ctrip.framework.drc.console.dao.ColumnsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.sql.SQLException;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;


public class ColumnsFilterServiceImplTest {

    @InjectMocks
    private ColumnsFilterServiceImpl columnsFilterService;

    @Mock private ColumnsFilterTblDao columnsFilterTblDao;
    

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        ColumnsFilterTbl mock = new ColumnsFilterTbl();
        mock.setId(1L);
        mock.setDataMediaId(1L);
        mock.setMode("exclude");
        mock.setColumns(JsonUtils.toJson(Lists.newArrayList("column1")));
        Mockito.when(columnsFilterTblDao.queryByDataMediaId(Mockito.anyLong(),Mockito.anyInt())).thenReturn(mock);
        
    }

    @Test
    public void testGenerateColumnsFilterConfig() throws SQLException {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(1L);
        dataMediaTbl.setNamespcae("db1");
        dataMediaTbl.setName("t1");
        ColumnsFilterConfig columnsFilterConfig = columnsFilterService.generateColumnsFilterConfig(dataMediaTbl);
        Assert.assertEquals(1,columnsFilterConfig.getColumns().size());
    }

    @Test
    public void testAddColumnsFilterConfig() throws SQLException {
        Mockito.when(columnsFilterTblDao.insert(Mockito.any(ColumnsFilterTbl.class))).thenReturn(1);
        ColumnsFilterConfigDto columnsFilterConfigDto = mockDto();
        columnsFilterConfigDto.setId(0L);
        columnsFilterService.addColumnsFilterConfig(columnsFilterConfigDto);
    }

    @Test
    public void testUpdateColumnsFilterConfig() throws SQLException{
        Mockito.when(columnsFilterTblDao.update(Mockito.any(ColumnsFilterTbl.class))).thenReturn(1);
        columnsFilterService.updateColumnsFilterConfig(mockDto());
    }

    @Test
    public void testDeleteColumnsFilterConfig() throws SQLException{
        Mockito.when(columnsFilterTblDao.update(Mockito.any(ColumnsFilterTbl.class))).thenReturn(1);
        columnsFilterService.deleteColumnsFilter(1L);
    }
    
    private ColumnsFilterConfigDto mockDto() {
        ColumnsFilterConfigDto dto = new ColumnsFilterConfigDto();
        dto.setId(1L);
        dto.setDataMediaId(1L);
        dto.setColumns(Lists.newArrayList("column1"));
        dto.setMode("exclude");
        return dto;
    }
}