package com.ctrip.framework.drc.console.enums;

import org.junit.Assert;
import org.junit.Test;

public class TableEnumTest {

    @Test
    public void testTableEnum() throws Exception {
        for(TableEnum tableEnum : TableEnum.values()) {
            Assert.assertNotEquals(0, tableEnum.getAllPojos().size());
        }
    }
}
