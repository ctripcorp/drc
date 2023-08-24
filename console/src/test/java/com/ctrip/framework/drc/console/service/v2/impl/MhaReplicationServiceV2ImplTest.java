package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.core.http.PageResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;


@RunWith(MockitoJUnitRunner.class)
public class MhaReplicationServiceV2ImplTest {


    @InjectMocks
    private MhaReplicationServiceV2Impl mhaReplicationServiceV2;

    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;


    @Before
    public void setup() throws SQLException {
        Mockito.when(mhaReplicationTblDao.queryByPage(Mockito.any())).thenReturn(Collections.singletonList(new MhaReplicationTbl()));
        Mockito.when(mhaReplicationTblDao.count(new MhaReplicationQuery())).thenReturn(1);
        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId(Mockito.any())).thenReturn(Collections.singletonList(new MhaReplicationTbl()));
    }

    @Test
    public void testQueryByPage() {
        PageResult<MhaReplicationTbl> pageResult = mhaReplicationServiceV2.queryByPage(new MhaReplicationQuery());

        Assert.assertNotNull(pageResult);

        Assert.assertFalse(CollectionUtils.isEmpty(pageResult.getData()));
        Assert.assertEquals(1, pageResult.getTotalCount());
    }

    @Test
    public void testQueryRelatedReplications() {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(1L));

        Assert.assertNotNull(mhaReplicationTbls);
        Assert.assertFalse(CollectionUtils.isEmpty(mhaReplicationTbls));

    }
}