package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetaGeneratorV5Test extends CommonDataInit {

    @InjectMocks
    MetaGeneratorV5 metaGeneratorV5;

    @Spy
    RowsFilterServiceV2 rowsFilterServiceV2 = new RowsFilterServiceV2Impl();

    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
    }

    @Test
    public void testGetDrc() throws Exception {
        Mockito.when(defaultConsoleConfig.getMetaGeneratorV5Switch()).thenReturn(true);
        Drc result = metaGeneratorV5.getDrc();
        String xml = XmlUtils.formatXML(result.toString());
        System.out.println(xml);
        Assert.assertNotEquals(0, result.getDcs().size());
        List<Applier> appliers = result.getDcs().values().stream().flatMap(e -> e.getDbClusters().values().stream()).flatMap(e -> e.getAppliers().stream()).collect(Collectors.toList());
        List<Messenger> messengers = result.getDcs().values().stream().flatMap(e -> e.getDbClusters().values().stream()).flatMap(e -> e.getMessengers().stream()).collect(Collectors.toList());
        Assert.assertTrue(appliers.stream().anyMatch(e -> e.getApplyMode().equals(ApplyMode.db_transaction_table.getType()) && !StringUtils.isEmpty(e.getIncludedDbs())));
        Assert.assertTrue(appliers.stream().anyMatch(e -> !e.getApplyMode().equals(ApplyMode.db_transaction_table.getType())));
        Assert.assertTrue(messengers.stream().anyMatch(e -> e.getApplyMode().equals(ApplyMode.db_mq.getType()) && !StringUtils.isEmpty(e.getIncludedDbs())));
        Assert.assertTrue(messengers.stream().anyMatch(e -> !e.getApplyMode().equals(ApplyMode.db_mq.getType())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate() {
        Map<Long, MhaTblV2> mhaDbMappingId2MhaTblMap = new HashMap<>();
        List<MhaDbReplicationTbl> mhaDbReplicationTbls = new ArrayList<>();
        metaGeneratorV5.validate(mhaDbMappingId2MhaTblMap, mhaDbReplicationTbls);

        mhaDbMappingId2MhaTblMap.put(1000L, new MhaTblV2());
        mhaDbMappingId2MhaTblMap.put(2000L, new MhaTblV2());
        metaGeneratorV5.validate(mhaDbMappingId2MhaTblMap, mhaDbReplicationTbls);
        MhaDbReplicationTbl tbl = new MhaDbReplicationTbl();
        tbl.setId(1L);
        tbl.setSrcMhaDbMappingId(1000L);
        tbl.setDstMhaDbMappingId(2000L);
        tbl.setReplicationType(0);
        mhaDbReplicationTbls.add(tbl);
        metaGeneratorV5.validate(mhaDbMappingId2MhaTblMap, mhaDbReplicationTbls);

        tbl.setDstMhaDbMappingId(2001L);
        metaGeneratorV5.validate(mhaDbMappingId2MhaTblMap, mhaDbReplicationTbls);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme