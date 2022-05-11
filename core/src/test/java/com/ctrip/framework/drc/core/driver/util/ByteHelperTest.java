package com.ctrip.framework.drc.core.driver.util;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by jixinwang on 2022/5/8
 */
public class ByteHelperTest {

    @Test
    public void writeInt() throws IOException {

        long intPart = 15;
        long hms = intPart % (1 << 3);
        System.out.println(hms);

//        final ByteArrayOutputStream out = new ByteArrayOutputStream();
//        ByteHelper.writeShortLittleEndian( -32766, out);
//        byte[] bytes = out.toByteArray();
//        System.out.println("bytes are: " + byteToHexString(bytes));
//
//        int value = ByteHelper.readUnsignedShortLittleEndian(bytes, 0);
//        System.out.println("value is: " + value);
//        String r1 = Integer.toHexString(-7);
//        System.out.println(r1);
    }


    private String byteToHexString(byte[] bytes)
    {
        StringBuilder resultHexString = new StringBuilder();
        String tempStr;
        for (byte b: bytes) {
            //这里需要对b与0xff做位与运算，
            //若b为负数，强制转换将高位位扩展，导致错误，
            //故需要高位清零
            tempStr = Integer.toHexString(b & 0xff);
            //若转换后的十六进制数字只有一位，
            //则在前补"0"
            if (tempStr.length() == 1) {
                resultHexString.append(0).append(tempStr);
            } else {
                resultHexString.append(tempStr);
            }
        }
        return resultHexString.toString();
    }
}
