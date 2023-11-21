package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.core.http.PageReq;
import com.ctrip.platform.dal.dao.DalTableDao;
import com.google.common.collect.Lists;
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
 * 2023/11/21 15:05
 */
public class ConflictRowsLogTblDaoTest {

    @InjectMocks
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Mock
    private DalTableDao<ConflictRowsLogTbl> client;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testQueryByParam() throws SQLException {
        Mockito.when(client.count(Mockito.any(), Mockito.any())).thenReturn(1);
        Mockito.when(client.query(Mockito.any(), Mockito.any())).thenReturn(new ArrayList<>());

        ConflictRowsLogQueryParam param = new ConflictRowsLogQueryParam();
        param.setBeginHandleTime(0L);
        param.setEndHandleTime(1L);
        param.setConflictTrxLogId(0L);
        param.setGtid("gtid");
        param.setDbsWithPermission(Lists.newArrayList("DB"));
        param.setDbName("db");
        param.setTableName("tableName");
        param.setSrcRegion("srcRegion");
        param.setDstRegion("dstRegion");
        param.setRowResult(0);
        param.setBrief(0);
        param.setPageReq(new PageReq());

        List<ConflictRowsLogTbl> result = conflictRowsLogTblDao.queryByParam(param);
        Assert.assertEquals(result.size(), 0);

        param.setLikeSearch(true);
        result = conflictRowsLogTblDao.queryByParam(param);
        Assert.assertEquals(result.size(), 0);

    }

}
