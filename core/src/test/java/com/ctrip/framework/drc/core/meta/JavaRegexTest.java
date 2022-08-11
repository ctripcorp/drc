package com.ctrip.framework.drc.core.meta;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Created by jixinwang on 2022/5/25
 */
public class JavaRegexTest {

    @Test
    public void testRegex() {
        boolean result = Pattern.matches(".*", "drc1.insert1");
        Assert.assertTrue(result);
    }
}
