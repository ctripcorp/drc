package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.service.ColumnsFilterService;
import com.ctrip.framework.drc.console.service.impl.ColumnsFilterServiceImpl;
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
import java.util.List;
import java.util.stream.Collectors;

public class MetaGeneratorV5Test extends CommonDataInit {

    @InjectMocks
    MetaGeneratorV5 metaGeneratorV5;

    @Spy
    RowsFilterServiceV2 rowsFilterServiceV2 = new RowsFilterServiceV2Impl();

    @Spy
    ColumnsFilterService columnsFilterService = new ColumnsFilterServiceImpl();

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
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme