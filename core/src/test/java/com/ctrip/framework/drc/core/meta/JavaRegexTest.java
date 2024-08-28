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
        Pattern pattern = Pattern.compile("^(?!(?i)CN_90001$|CN_90002$|CN_90003$).*$");
        Matcher matcher =  pattern.matcher("CN_90002");
        boolean result = matcher.find();
        Assert.assertFalse(result);

        Matcher matcher2 =  pattern.matcher("cn_90001");
        boolean result2 = matcher2.find();
        Assert.assertFalse(result2);

        Matcher matcher3 =  pattern.matcher("SSS");
        boolean result3 = matcher3.find();
        Assert.assertTrue(result3);
    }
}
