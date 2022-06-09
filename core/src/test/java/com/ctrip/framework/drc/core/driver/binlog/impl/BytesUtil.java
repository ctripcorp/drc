package com.ctrip.framework.drc.core.driver.binlog.impl;

import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Assert;

import java.util.stream.IntStream;

/**
 * Created by @author zhuYongMing on 2019/9/27.
 */
public class BytesUtil {

    public static byte[] toBytesFromHexString(String hexString) {
        // mac control+command+G multi rows operation
        hexString = hexString.replaceAll(" ", "");
        hexString = hexString.replaceAll("\n", "");
        return HexUtils.fromHexString(hexString);
    }

    public static byte[] toBytesFromHexString(final String hexString, final int index, final int length) {
        final byte[] source = toBytesFromHexString(hexString);
        return splitBytes(source, index, length);
    }

    public static byte[] splitBytes(final byte[] source, final int index, final int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(source, index, bytes, 0, length);
        return bytes;
    }

    public static void bytesAssert(final byte[] expected, final byte[] actual) {
        if (expected.length != actual.length) {
            Assert.fail();
        }
        IntStream.range(0, expected.length).forEach(
                index -> Assert.assertEquals(expected[index], actual[index])
        );
    }

    public static String toHexString(String hexString) {
        return hexString.replaceAll(" ", "").replaceAll("\n", "");
    }

    public static byte[] toBytesFromDecimalString(String decimalString) {
        decimalString = decimalString.replaceAll("\n", "");
        final String[] strings = decimalString.split(", ");
        byte[] bytes = new byte[strings.length];
        for (int i = 0; i < strings.length; i++) {
            bytes[i] = Integer.valueOf(strings[i]).byteValue();
        }

        return bytes;
    }
}
