package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.core.http.PageResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;


public class MhaReplicationServiceV2ImplTest extends CommonDataInit {


    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
    }

    @Test
    public void testQueryByPage() {
        PageResult<MhaReplicationTbl> pageResult = mhaReplicationServiceV2.queryByPage(new MhaReplicationQuery());

        Assert.assertNotNull(pageResult);

        Assert.assertFalse(CollectionUtils.isEmpty(pageResult.getData()));
        Assert.assertNotEquals(0, pageResult.getTotalCount());
    }

    @Test
    public void testQueryRelatedReplications() {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(1L));

        Assert.assertNotNull(mhaReplicationTbls);
        Assert.assertFalse(CollectionUtils.isEmpty(mhaReplicationTbls));

    }

    @Test
    public void testQueryRelatedReplicationsByDbNames() {

        List<String> dbNames = Lists.newArrayList("db1", "db2");
        String mhaName = "mha1";
        List<String> mhaNames = Lists.newArrayList(mhaName);
        List<MhaReplicationDto> mhaReplicationDtos = mhaReplicationServiceV2.queryRelatedReplications(mhaNames, dbNames);

        Assert.assertEquals(3, mhaReplicationDtos.size());
        for (MhaReplicationDto replicationDto : mhaReplicationDtos) {
            Assert.assertTrue(replicationDto.getSrcMha().getName().equals(mhaName) || replicationDto.getDstMha().getName().equals(mhaName));
            Assert.assertNotEquals(0, replicationDto.getDbs().size());
            Assert.assertTrue(dbNames.containsAll(replicationDto.getDbs()));
        }
    }

    @Test
    public void testGetMhaReplicationSingleDelay() {
        String mha1 = "mha1";
        String mha2 = "mha2";
        long srcNowTime = 205L;
        long dstLastUpdateTime = 100L;
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha2)).thenReturn(dstLastUpdateTime);
        when(mysqlServiceV2.getCurrentTime(mha1)).thenReturn(srcNowTime);
        MhaDelayInfoDto delay = mhaReplicationServiceV2.getMhaReplicationDelay(mha1, mha2);

        Assert.assertNotNull(delay);
        Assert.assertEquals(mha1, delay.getSrcMha());
        Assert.assertEquals(mha2, delay.getDstMha());
        long delayExpected = srcNowTime - dstLastUpdateTime;
        Assert.assertEquals(delayExpected, delay.getDelay().longValue());
        System.out.println(delay);
    }

    @Test
    public void testGetMhaReplicationSingleDelays() {
        String mha1 = "mha1";
        String mha2 = "mha2";
        String mha3 = "mha3";
        long srcNowTime = 205L;
        long mha1To2UpdateTIme = 50L;
        long mha1To3UpdateTIme = 100L;
        List<MhaReplicationDto> list = new ArrayList<>();


        when(mysqlServiceV2.getCurrentTime(mha1)).thenReturn(srcNowTime);
        list.add(MhaReplicationDto.from("mha1", "mha2"));
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha2)).thenReturn(mha1To2UpdateTIme);
        list.add(MhaReplicationDto.from("mha1", "mha3"));
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha3)).thenReturn(mha1To3UpdateTIme);

        List<MhaDelayInfoDto> delay = mhaReplicationServiceV2.getMhaReplicationDelays(list);

        for (MhaDelayInfoDto infoDTO : delay) {
            Long expectDelay = null;
            if (infoDTO.getSrcMha().equals(mha1) && infoDTO.getDstMha().equals(mha2)) {
                expectDelay = srcNowTime - mha1To2UpdateTIme;
            } else if (infoDTO.getSrcMha().equals(mha1) && infoDTO.getDstMha().equals(mha3)) {
                expectDelay = srcNowTime - mha1To3UpdateTIme;
            }
            Assert.assertEquals(expectDelay, infoDTO.getDelay());
        }
        Assert.assertNotNull(delay);
        Assert.assertEquals(list.size(), delay.size());
        System.out.println(delay);
    }

}