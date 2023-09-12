package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.AbstractIntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class MhaReplicationTblDaoTest extends AbstractIntegrationTest {

    @Autowired
    MhaReplicationTblDao mhaReplicationTblDao;

    private void insertReplications() throws SQLException {
        List<MhaReplicationTbl> mhaReplicationTbls = new ArrayList<>();
        int[][] pairs = new int[][]{{1, 2}, {2, 1}, {1, 3}};
        for (int i = 1; i <= 50; i++) {
            MhaReplicationTbl tbl = new MhaReplicationTbl();
            tbl.setId((long) i);
            tbl.setSrcMhaId((long) pairs[i][0]);
            tbl.setDstMhaId((long) pairs[i][1]);
            tbl.setDeleted(0);
            tbl.setCreateTime(new Timestamp(new java.util.Date().getTime()));
            tbl.setDatachangeLasttime(new Timestamp(new java.util.Date().getTime()));
            tbl.setDrcStatus(1);
            mhaReplicationTbls.add(tbl);
        }

        mhaReplicationTblDao.insert(mhaReplicationTbls);
    }

    @Test
    public void testQueryByPage() throws Exception {
        insertReplications();
        List<MhaReplicationTbl> list = mhaReplicationTblDao.queryByPage(new MhaReplicationQuery());
        int count = mhaReplicationTblDao.count(new MhaReplicationQuery());
        Assert.assertEquals(3, count);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme