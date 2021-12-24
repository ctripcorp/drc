package com.ctrip.framework.drc.core.monitor.enums;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/6/16
 */
public class ModuleEnumTest {

    @Test
    public void testDescription() {
        for (ModuleEnum moduleEnum : ModuleEnum.values()) {
            String literal = moduleEnum.toString();
            String[] parts = literal.split("_");
            StringBuilder stringBuilder = new StringBuilder();
            for (String s : parts) {
                stringBuilder.append(s.charAt(0));
            }
            Assert.assertEquals(stringBuilder.toString(), moduleEnum.getDescription());
        }
    }
}