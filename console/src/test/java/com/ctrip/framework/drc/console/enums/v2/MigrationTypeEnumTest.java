package com.ctrip.framework.drc.console.enums.v2;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by shiruixin
 * 2025/5/20 10:52
 */
public class MigrationTypeEnumTest {
    @Test
    public void testisCommon() {
        boolean res = MigrationTypeEnum.OVERSEA_START_SHA_TO_OVERSEA.isCommon();
        assertFalse(res);
        res = MigrationTypeEnum.COMMON_START.isCommon();
        assertTrue(res);
    }
}