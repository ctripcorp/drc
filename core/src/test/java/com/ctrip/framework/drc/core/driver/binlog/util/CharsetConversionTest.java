package com.ctrip.framework.drc.core.driver.binlog.util;

import com.ctrip.framework.drc.core.driver.util.CharsetConversion;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by @author zhuYongMing on 2019/9/20.
 */
public class CharsetConversionTest {

    @Test
    public void getCharsetTest() {
        String charset = CharsetConversion.getJavaCharset("utf8", "utf8_unicode_ci");
        Assert.assertEquals("UTF-8", charset);
    }

}
