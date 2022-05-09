package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

public class DataMediaServiceImplTest extends AbstractTest {

    @Mock
    private DataMediaTblDao dataMediaTblDao;

    @Mock
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;

    @InjectMocks
    private DataMediaServiceImpl dataMediaService;
    
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testAddDataMedia() throws SQLException {
        DataMediaDto dataMediaDto = new DataMediaDto();
        dataMediaDto.setNamespace("db[01-32]");
        dataMediaDto.setName("table1|table2");
        dataMediaDto.setType(DataMediaTypeEnum.REGEX_LOGIC.getType());
        dataMediaDto.setDataMediaSourceId(1L);
        Mockito.when(dataMediaTblDao.insert(Mockito.any(DataMediaTbl.class))).thenReturn(1);

        String result = dataMediaService.addDataMedia(new DataMediaDto());
        Assert.assertEquals("illegal argument for DataMedia",result);
        result = dataMediaService.addDataMedia(dataMediaDto);
        Assert.assertEquals("add DataMedia success",result);
    }

    @Test
    public void testAddDataMediaMapping() throws SQLException {
        Mockito.when(rowsFilterMappingTblDao.insert(Mockito.any(RowsFilterMappingTbl.class))).thenReturn(1);
        Assert.assertEquals(
                "add DataMediaMapping success",
                dataMediaService.addDataMediaMapping(1L,1L));
    }
    
    @Test
    public void testDeleteDataMediaMapping() {
    }
}