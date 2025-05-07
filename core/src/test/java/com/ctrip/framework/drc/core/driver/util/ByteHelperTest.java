package com.ctrip.framework.drc.core.driver.util;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.FormatDescriptionLogEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.AsciiString;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

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

    @Test
    public void testWriteLengthEncodeInt() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeLengthEncodeInt(0, out);
        ByteHelper.writeLengthEncodeInt(251, out);
        ByteHelper.writeLengthEncodeInt(255, out);
        ByteHelper.writeLengthEncodeInt(256, out);
        ByteHelper.writeLengthEncodeInt(123456, out);


        ByteBuf byteBuf = getByteBuf(out.toByteArray());
        long num0 = readLengthEncodeInt(byteBuf);
        long num1 = readLengthEncodeInt(byteBuf);
        long num2 = readLengthEncodeInt(byteBuf);
        long num3 = readLengthEncodeInt(byteBuf);
        long num4 = readLengthEncodeInt(byteBuf);

        Assert.assertEquals(0, num0);
        Assert.assertEquals(251, num1);
        Assert.assertEquals(255, num2);
        Assert.assertEquals(256, num3);
        Assert.assertEquals(123456, num4);
    }

    @Test
    public void testWriteFixedLengthBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String value = getValue();
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ByteHelper.writeUnsignedShortLittleEndian(bytes.length, out);
        ByteHelper.writeFixedLengthBytes(bytes, 0, bytes.length, out);


        ByteBuf byteBuf = getByteBuf(out.toByteArray());
        int valueLength = byteBuf.readUnsignedShortLE();
        String value1 = readFixLengthString(byteBuf, valueLength, StandardCharsets.UTF_8);
        Assert.assertEquals(value, value1);

    }

    @Test
    public void testWriteVariablesLengthStringDefaultCharset() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeVariablesLengthStringDefaultCharset(getValue(), out);
    }

    private String readVariableLengthStringDefaultCharset(final ByteBuf byteBuf) {
        // mysql default charset is latin1, map to java is IOS_8859_1
        final int length = (int) readLengthEncodeInt(byteBuf);
        return readFixLengthStringDefaultCharset(byteBuf, length);
    }

    protected String readFixLengthStringDefaultCharset(final ByteBuf byteBuf, final int length) {
        // mysql default charset is latin1, map to java is IOS_8859_1
        return readFixLengthString(byteBuf, length, ISO_8859_1);
    }

    private String getValue() {
        String val = "a";
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 256 * 256 - 1; i++) {
            stringBuilder.append(val);
        }
        return stringBuilder.toString();
    }

    private String readFixLengthString(final ByteBuf byteBuf, final int length, final Charset charset) {
        final CharSequence string = byteBuf.readCharSequence(length, charset);

        if (string instanceof AsciiString) {
            final AsciiString asciiString = (AsciiString) string;
            return asciiString.toString();
        }
        return string.toString();
    }

    private ByteBuf getByteBuf(byte[] bytes) throws IOException {
        final byte[] payloadBytes = bytes;
        final int payloadLength = payloadBytes.length;
        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
//        payloadByteBuf.skipBytes(payloadLength);
        return payloadByteBuf;
    }

    private long readLengthEncodeInt(final ByteBuf byteBuf) {
        final int encodeLength = byteBuf.readUnsignedByte();
        if (encodeLength < 252) {
            return encodeLength;
        }

        switch (encodeLength) {
            case 252:
                return byteBuf.readUnsignedShortLE(); // 2bytes
            case 253:
                return byteBuf.readUnsignedMediumLE(); // 3bytes
            case 254:
                // TODO: 2019/9/11 may be ERR_Packet
                return byteBuf.readLongLE(); // 8bytes
            default:
                // TODO: 2019/9/8 ERR_Packet
                return 0;
        }
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
