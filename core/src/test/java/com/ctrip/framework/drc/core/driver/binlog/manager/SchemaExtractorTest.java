package com.ctrip.framework.drc.core.driver.binlog.manager;

import org.junit.Assert;
import org.junit.Test;

public class SchemaExtractorTest {

    @Test
    public void testConvertColumnDefault() {
        Assert.assertEquals("columnDefault", SchemaExtractor.convertColumnDefault(true, "columnDefault", "dataTypeLiteral"));
        Assert.assertEquals("0x62696E61727932303032", SchemaExtractor.convertColumnDefault(false, "0x62696E61727932303032", "binary"));
        Assert.assertEquals("0x62696E61727932303032", SchemaExtractor.convertColumnDefault(true, "0x62696E61727932303032", "varchar"));
        Assert.assertEquals("binary2002", SchemaExtractor.convertColumnDefault(true, "0x62696E61727932303032", "binary"));
        Assert.assertEquals("binary2002", SchemaExtractor.convertColumnDefault(true, "0X62696e61727932303032", "binary"));
        Assert.assertEquals("binary2002", SchemaExtractor.convertColumnDefault(true, "0X62696E61727932303032", "varbinary"));
    }
}
