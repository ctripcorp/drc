package com.ctrip.framework.drc.core.driver.binlog.json;

import java.io.*;
import java.util.List;

import com.ctrip.framework.drc.core.driver.binlog.impl.BytesUtil;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

public class BinaryJsonTest {
    
    @Test
    public void testParseAndLoad() throws IOException {
        final ByteBuf byteBuf = initByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockTableInfo());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1,beforePresentRowsValues.size());

        final List<Object> values = beforePresentRowsValues.get(0);
        Assert.assertEquals(3,values.size());
        Assert.assertEquals(2,values.get(0));
        Assert.assertEquals("[0,-32768,65535,-2147483648,4294967295,-9223372036854775808,18446744073709551615]",
                new BinaryJson((byte [])values.get(1)).parseAsString());
        Assert.assertEquals("2024-03-04 18:59:09.006",values.get(2));


    }


    /**
     * mysql> select * from drc1.json;
     * +----+-----------------------------------------------------------------------------------------+-------------------------+
     * | id | json_data                                                                               | datachange_lasttime     |
     * +----+-----------------------------------------------------------------------------------------+-------------------------+
     * |  2 | [0, -32768, 65535, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615] | 2024-03-04 17:18:54.931 |
     * +----+-----------------------------------------------------------------------------------------+-------------------------+
     * @return
     * @throws IOException
     */
    private ByteBuf initByteBuf() throws IOException {
        // metadata lenth = 58,final int valueLength = (int) byteBuf.readUnsignedIntLE();
        String hexString = "7da9e5651e640000006c000000ec680a0000007900000000000100020003ff00020000003a0000000207003900050000050080071900071d000921000929000a3100ffff000000000080ffffffff000000000000000000000080ffffffffffffffff65e5a97d003c4f0bf2cb";
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);
        return byteBuf;

    }

    private List<TableMapLogEvent.Column> mockTableInfo() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column id = new TableMapLogEvent.Column("id", false, "int", null, null, null, null, null, null, "int", "PRI", "auto_increment", "NULL");
        TableMapLogEvent.Column json = new TableMapLogEvent.Column("json", true, "json", null, null, null, null, null, null, "json", null, null, "NULL");
        TableMapLogEvent.Column time = new TableMapLogEvent.Column("datachange_lasttime", false, "timestamp", null, null, null, "3", null, null, "timestamp(3)", null, "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3) ");
        json.setMeta(4);
        columns.add(id);
        columns.add(json);
        columns.add(time);
        return columns;
    }
    @Test
    public void testParseBoolean() throws IOException {
        String hexString = "02 03 00 0D 00 04 00 00 04 01 00 04 02 00";
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String s = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("[null,true,false]",s);
    }

    @Test
    public void testParseInt16() throws IOException {

        String hexString = "02 07 00 39 00 05 00 00 05 00 80 07 19 00 07 1D 00 09 21 00 09 29 00 0A 31 00 FF FF 00 00 00 00 00 80 FF FF FF FF 00 00 00 00 00 00 00 00 00 00 00 80 FF FF FF FF FF FF FF FF \n";
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String s = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("[0,-32768,65535,-2147483648,4294967295,-9223372036854775808,18446744073709551615]",s);
    }

    @Test
    public void testParseString() throws IOException {
        // long String
        String hexString = "00 01 00 CD 01 0B 00 0A 00 0C 15 00 6C 6F 6E 67 53 74 72 69 6E 67 B6 03 70 72 6F 74 65 63 74 65 64 20 76 6F 69 64 20 64 6F 57 69 74 68 53 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 28 53 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 3C 4E 65 74 74 79 43 6C 69 65 6E 74 3E 20 73 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 2C 20 44 75 6D 70 43 61 6C 6C 42 61 63 6B 20 63 61 6C 6C 42 61 63 6B 2C 20 52 45 43 4F 4E 4E 45 43 54 49 4F 4E 5F 43 4F 44 45 20 72 65 63 6F 6E 6E 65 63 74 69 6F 6E 43 6F 64 65 29 20 7B 70 72 6F 74 65 63 74 65 64 20 76 6F 69 64 20 64 6F 57 69 74 68 53 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 28 53 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 3C 4E 65 74 74 79 43 6C 69 65 6E 74 3E 20 73 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 2C 20 44 75 6D 70 43 61 6C 6C 42 61 63 6B 20 63 61 6C 6C 42 61 63 6B 2C 20 52 45 43 4F 4E 4E 45 43 54 49 4F 4E 5F 43 4F 44 45 20 72 65 63 6F 6E 6E 65 63 74 69 6F 6E 43 6F 64 65 29 20 7B 70 72 6F 74 65 63 74 65 64 20 76 6F 69 64 20 64 6F 57 69 74 68 53 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 28 53 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 3C 4E 65 74 74 79 43 6C 69 65 6E 74 3E 20 73 69 6D 70 6C 65 4F 62 6A 65 63 74 50 6F 6F 6C 2C 20 44 75 6D 70 43 61 6C 6C 42 61 63 6B 20 63 61 6C 6C 42 61 63 6B 2C 20 52 45 43 4F 4E 4E 45 43 54 49 4F 4E 5F 43 4F 44 45 20 72 65 63 6F 6E 6E 65 63 74 69 6F 6E 43 6F 64 65 29 20 7B ";
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String res = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("{\"longString\":\"protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode) {protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode) {protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode) {\"}",res);

        // short String and escape
        hexString="00 02 00 34 00 12 00 02 00 14 00 02 00 0C 16 00 0C 23 00 63 6E 65 6E 0C E4 B8 AD E6 96 87 E6 B5 8B E8 AF 95 10 74 65 73 74 5F 6A 73 6F 6E 21 40 23 24 7E 60 0A ";
        final byte[] bytes2 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes2).parseAsString();
        Assert.assertEquals("{\"cn\":\"中文测试\",\"en\":\"test_json!@#$~`\\n\"}",res);
    }

    @Test
    public void testParseDouble() throws IOException {
        String hexString = "02 04 00 28 00 05 00 00 0B 10 00 0B 18 00 0B 20 00 4F AF 94 65 88 E3 08 40 FF FF FF FF FF FF EF FF FF FF FF FF FF FF EF 7F";
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String res = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("[0,3.1111,-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368,179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368]",res);
    }

    @Test
    public void testParseTime() throws IOException {
        // time
        String hexString ="00 01 00 1F 00 0B 00 04 00 0C 0F 00 74 69 6D 65 0F 30 30 3A 30 30 3A 30 30 2E 30 30 30 30 30 30";
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String res = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("{\"time\":\"00:00:00.000000\"}", res);

        hexString = "00 01 00 21 00 0B 00 04 00 0C 0F 00 74 69 6D 65 11 2D 38 33 38 3A 35 39 3A 35 39 2E 30 30 30 30 30 30";
        byte[] bytes1 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes1).parseAsString();
        Assert.assertEquals("{\"time\":\"-838:59:59.000000\"}",res);


        hexString = "00 01 00 20 00 0B 00 04 00 0C 0F 00 74 69 6D 65 10 38 33 38 3A 35 39 3A 35 39 2E 30 30 30 30 30 30";
        byte[] bytes2 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes2).parseAsString();
        Assert.assertEquals("{\"time\":\"838:59:59.000000\"}",res);
    }

    @Test
    public void testParseDate() throws IOException {
        String hexString ="00 01 00 19 00 0B 00 04 00 0F 0F 00 64 61 74 65 0A 08 00 00 00 00 00 42 B2 0C";
        byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String res = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("{\"date\":\"1000-01-01\"}", res);

        hexString = "00 01 00 19 00 0B 00 04 00 0F 0F 00 64 61 74 65 0A 08 00 00 00 00 00 FE F3 7E";
        byte[] bytes1 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes1).parseAsString();
        Assert.assertEquals("{\"date\":\"9999-12-31\"}",res);
    }

    @Test
    public void testParseDateTime() throws IOException {
        String hexString ="00 01 00 1D 00 0B 00 08 00 0F 13 00 64 61 74 65 74 69 6D 65 0C 08 00 00 00 00 00 42 B2 0C";
        byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String res = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("{\"datetime\":\"1000-01-01 00:00:00\"}", res);

        hexString = "00 01 00 1D 00 0B 00 08 00 0F 13 00 64 61 74 65 74 69 6D 65 0C 08 1F A1 07 FB 7E FF F3 7E";
        byte[] bytes1 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes1).parseAsString();
        Assert.assertEquals("{\"datetime\":\"9999-12-31 23:59:59.499999\"}",res);

        hexString = "00 01 00 1E 00 0B 00 09 00 0F 14 00 74 69 6D 65 73 74 61 6D 70 0C 08 00 00 00 01 00 C2 02 19";
        byte[] bytes2 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes2).parseAsString();
        Assert.assertEquals("{\"timestamp\":\"1970-01-01 00:00:01\"}",res);


        hexString = "00 01 00 1E 00 0B 00 09 00 0F 14 00 74 69 6D 65 73 74 61 6D 70 0C 08 1F A1 07 87 33 E6 DF 19";
        byte[] byte3 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(byte3).parseAsString();
        Assert.assertEquals("{\"timestamp\":\"2038-01-19 03:14:07.499999\"}",res);
    }

    @Test
    public void testParseDecimal() throws IOException {
        String hexString ="00 01 00 34 00 0B 00 07 00 0F 12 00 64 65 63 69 6D 61 6C F6 20 42 1E 85 F5 E0 FF 3B 9A C9 FF 3B 9A C9 FF 3B 9A C9 FF 3B 9A C9 FF 3B 9A C9 FF 3B 9A C9 FF 03 E7";
        byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        String res = new BinaryJson(bytes).parseAsString();
        Assert.assertEquals("{\"decimal\":99999999999999999999999999999999999.999999999999999999999999999999}", res);

        hexString = "00 01 00 34 00 0B 00 07 00 0F 12 00 64 65 63 69 6D 61 6C F6 20 42 1E 7A 0A 1F 00 C4 65 36 00 C4 65 36 00 C4 65 36 00 C4 65 36 00 C4 65 36 00 C4 65 36 00 FC 18";
        byte[] bytes1 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes1).parseAsString();
        Assert.assertEquals("{\"decimal\":-99999999999999999999999999999999999.999999999999999999999999999999}",res);

        hexString = "00 01 00 28 00 0B 00 07 00 0F 12 00 64 65 63 69 6D 61 6C F6 14 27 1E 80 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00";
        byte[] bytes2 = BytesUtil.toBytesFromHexString(hexString);
        res = new BinaryJson(bytes2).parseAsString();
        Assert.assertEquals("{\"decimal\":0.000000000000000000000000000000}",res);
    }

    @Test
    public void testParseLargeArray() throws IOException {
        String path = getClass().getClassLoader().getResource("test/json/json_byte1.txt").getPath();
        try (FileReader fileReader = new FileReader(path)) {
            BufferedReader reader = new BufferedReader(fileReader);
            reader.readLine(); // skip
            String hexString = reader.readLine();
            reader.readLine(); // skip
            String exceptedString = reader.readLine();

            byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
            String res = new BinaryJson(bytes).parseAsString();

            Assert.assertEquals(exceptedString,res);
            reader.close();
        }
    }

    @Test
    public void testParseLargeObject() throws IOException {
        String path = getClass().getClassLoader().getResource("test/json/json_byte2.txt").getPath();
        try (FileReader fileReader = new FileReader(path)) {
            BufferedReader reader = new BufferedReader(fileReader);
            reader.readLine(); // skip
            String hexString = reader.readLine();
            reader.readLine(); // skip
            String exceptedString = reader.readLine();

            byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
            String res = new BinaryJson(bytes).parseAsString();

            Assert.assertEquals(exceptedString,res);
            reader.close();
        }
    }

    @Test
    public void testEscape() {
        StringBuilder test = new StringBuilder("\\\"a\nb\uD83D\uDE02中文");
        test.append('\n');
        test.append((char)0x7f);
        test.append((char)0xA0);
        BinaryJsonStringBuilder builder = new BinaryJsonStringBuilder();
        builder.appendString(test.toString());
        System.out.println(builder.getString());
        Assert.assertEquals("\\\\\\\"a\\nb\uD83D\uDE02中文\\n\\u007f\\u00a0", builder.getString());
    }
}