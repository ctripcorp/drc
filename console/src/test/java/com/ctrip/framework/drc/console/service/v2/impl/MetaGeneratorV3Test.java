package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.core.entity.Drc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.sql.SQLException;

public class MetaGeneratorV3Test extends CommonDataInit {

    @InjectMocks
    MetaGeneratorV3 metaGeneratorV3;

    @Spy
    RowsFilterServiceV2 rowsFilterServiceV2 = new RowsFilterServiceV2Impl();



    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
    }

    @Test
    public void testGetDrc() throws Exception {
        Drc result = metaGeneratorV3.getDrc();
        String xml = XmlUtils.formatXML(result.toString());
        System.out.println(xml);
        Assert.assertNotEquals(0, result.getDcs().size());


    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme