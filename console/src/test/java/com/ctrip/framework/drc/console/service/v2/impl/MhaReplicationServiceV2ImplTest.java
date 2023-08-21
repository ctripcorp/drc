package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.core.http.PageResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class MhaReplicationServiceV2ImplTest {


    @InjectMocks
    private MhaReplicationServiceV2Impl mhaReplicationServiceV2;

    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;

    @Mock
    private MhaTblV2Dao mhaTblV2Dao;

    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Mock
    private MysqlServiceV2 mysqlServiceV2;

    @Before
    public void setup() throws SQLException {
        when(mhaReplicationTblDao.queryByPage(any())).thenReturn(Collections.singletonList(new MhaReplicationTbl()));
        when(mhaReplicationTblDao.count(new MhaReplicationQuery())).thenReturn(1);
        when(mhaReplicationTblDao.queryByRelatedMhaId(any())).thenReturn(Collections.singletonList(new MhaReplicationTbl()));

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

    @Test
    public void testDelayQuery() throws SQLException {

        when(mhaTblV2Dao.queryByMhaNames(anyList())).thenAnswer(invocation -> {
            List<String> mhas = invocation.getArgument(0, List.class);
            return mhas.stream().map(e -> {
                MhaTblV2 tbl = new MhaTblV2();
                tbl.setMhaName(e);
                tbl.setDcId(1L);
                return tbl;
            }).collect(Collectors.toList());
        });


        String srcMha = "mha1";
        String dstMha = "mha2";

        Long dstTime = 100L;
        Long srcTime = 110L;
        when(mysqlServiceV2.getDelayUpdateTime(anyString(), anyString())).thenReturn(dstTime, srcTime);
        MhaDelayInfoDto ret = mhaReplicationServiceV2.getMhaReplicationDelay(srcMha, dstMha);
        Assert.assertEquals(dstTime, ret.getDstLastUpdateTime());
        Assert.assertEquals(srcTime, ret.getSrcLastUpdateTime());
        Assert.assertEquals((Long) (srcTime - dstTime), ret.getValue());

    }
}