package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.DdlHistoryTblDao;
import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;
import com.ctrip.framework.drc.console.service.v2.impl.CentralServiceImpl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaTblV2s;

/**
 * Created by dengquanliang
 * 2023/12/6 20:21
 */
public class CentralServiceTest {

    @InjectMocks
    private CentralServiceImpl centralService;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DdlHistoryTblDao ddlHistoryTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testInsertDdlHistory() throws SQLException {
        DdlHistoryEntity requestBody = new DdlHistoryEntity("mha", "ddl", 0, "db", "table");
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString())).thenReturn(getMhaTblV2s().get(0));
        Mockito.when(ddlHistoryTblDao.insert(Mockito.any(DalHints.class), Mockito.any(KeyHolder.class), Mockito.any(DdlHistoryTbl.class))).thenReturn(1);

        Integer result = centralService.insertDdlHistory(requestBody);
        Assert.assertTrue(result == 1);
    }
}
