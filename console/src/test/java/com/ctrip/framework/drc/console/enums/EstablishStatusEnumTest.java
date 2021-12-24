package com.ctrip.framework.drc.console.enums;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-26
 */
public class EstablishStatusEnumTest {
    @Test
    public void test() {
        for(EstablishStatusEnum establishStatusEnum : EstablishStatusEnum.values()) {
            EstablishStatusEnum actual = EstablishStatusEnum.getEnumByCode(establishStatusEnum.getCode());
            Assert.assertEquals(establishStatusEnum.getCode(), actual.getCode());
            Assert.assertEquals(establishStatusEnum.getDescription(), actual.getDescription());
        }
    }
}