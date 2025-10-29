package com.ctrip.framework.drc.console.vo.v2;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by shiruixin
 * 2025/5/12 19:06
 */
public class MqMetaCreateResultViewTest {

    @Test
    public void testMqMetaCreateResultView() {
        MqMetaCreateResultView view = new MqMetaCreateResultView("error");
        Assert.assertTrue(view.isFail());
        Assert.assertEquals("error", view.getErrMsg());
    }
}