package com.ctrip.framework.drc.core.meta;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jixinwang on 2022/5/25
 */
public class JavaRegexTest {

    @Test
    public void testRegex() {
        Pattern pattern = Pattern.compile("(?i)^12$");
        Matcher matcher =  pattern.matcher("12");
        boolean result = matcher.find();
        Assert.assertTrue(result);

        Matcher matcher2 =  pattern.matcher("1");
        boolean result2 = matcher2.find();
        Assert.assertFalse(result2);
    }
}
