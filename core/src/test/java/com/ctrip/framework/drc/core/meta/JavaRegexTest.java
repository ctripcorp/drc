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
    public void testRegex2() {
        Pattern oldTravixRegex = Pattern.compile("(?i)(?<!_TRAVIX)$");
        Pattern pattern = Pattern.compile("(?i)^(GB_90009)$");
        Assert.assertTrue(pattern.matcher("GB_90009").find());
        Assert.assertTrue(pattern.matcher("gb_90009").find());

        Assert.assertFalse(pattern.matcher("").find());
        Assert.assertFalse(pattern.matcher(" ").find());
        Assert.assertFalse(pattern.matcher("\n").find());
        Assert.assertFalse(pattern.matcher("abc").find());
        Assert.assertFalse(pattern.matcher("gb_90001").find());
        Assert.assertFalse(pattern.matcher("abcGB_90009").find());

        Assert.assertFalse(pattern.matcher("ABC_TRAVIX").find());
        Assert.assertFalse(pattern.matcher("ABC_Travix").find());
        Assert.assertFalse(pattern.matcher("_TRAVIX").find());


        Assert.assertFalse(oldTravixRegex.matcher("ABC_TRAVIX").find());
        Assert.assertFalse(oldTravixRegex.matcher("ABC_Travix").find());
        Assert.assertFalse(oldTravixRegex.matcher("_TRAVIX").find());
    }

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
