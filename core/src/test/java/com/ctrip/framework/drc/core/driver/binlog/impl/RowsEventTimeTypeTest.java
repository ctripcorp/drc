package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/10/14.
 */
public class RowsEventTimeTypeTest {

    @Test
    public void readTimeTypeTest() {
        final ByteBuf byteBuf = initTimeTypeByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockTimeTypeTable());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(9, beforePresentRows1Values.size());

        Assert.assertEquals("2019-10-16", beforePresentRows1Values.get(0));
        Assert.assertEquals("15:15:16", beforePresentRows1Values.get(1));
        Assert.assertEquals("15:15:15.666666", beforePresentRows1Values.get(2));
        Assert.assertEquals("2019-10-16 15:15:16", beforePresentRows1Values.get(3));
        Assert.assertEquals("2019-10-16 15:15:15.666666", beforePresentRows1Values.get(4));
        Assert.assertEquals("2019-10-16 15:15:16", beforePresentRows1Values.get(5));
        Assert.assertEquals("2019-10-16 15:15:15.666666", beforePresentRows1Values.get(6));
        Assert.assertEquals("2019", beforePresentRows1Values.get(7));
        Assert.assertEquals("2019", beforePresentRows1Values.get(8));
    }

    /**
     * sql:
     * insert into `drc4`.`time_type` values (
     * '2019-10-16 15:15:15.666666',
     * '15:15:15.666666',
     * '15:15:15.666666',
     * '2019-10-16 15:15:15.666666',
     * '2019-10-16 15:15:15.666666',
     * '2019-10-16 15:15:15.666666',
     * '2019-10-16 15:15:15.666666',
     * '2019',
     * '19'
     * );
     *
     * result:
     * mysql> select * from `drc4`.`time_type`;
     * +------------+----------+-----------------+---------------------+----------------------------+---------------------+----------------------------+------+-------+
     * | date       | time     | time6           | datetime            | datetime6                  | timestamp           | timestamp6                 | year | year4 |
     * +------------+----------+-----------------+---------------------+----------------------------+---------------------+----------------------------+------+-------+
     * | 2019-10-16 | 15:15:16 | 15:15:15.666666 | 2019-10-16 15:15:16 | 2019-10-16 15:15:15.666666 | 2019-10-16 15:15:16 | 2019-10-16 15:15:15.666666 | 2019 |  2019 |
     * +------------+----------+-----------------+---------------------+----------------------------+---------------------+----------------------------+------+-------+
     * 1 row in set (0.00 sec)
     */
    private ByteBuf initTimeTypeByteBuf() {
        String hexString =
                "73 c6 a6 5d 1e 01 00 00  00 4c 00 00 00 1e 79 00" +
                        "00 00 00 df 04 00 00 00  00 01 00 02 00 09 ff ff" +
                        "00 fe 50 c7 0f 80 f3 d0  80 f3 cf 0a 2c 2a 99 a4" +
                        "60 f3 d0 99 a4 60 f3 cf  0a 2c 2a 5d a6 c3 84 5d" +
                        "a6 c3 83 0a 2c 2a 77 77  c4 82 00 15";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }


    /**
     * table description:
     * mysql> desc `drc4`.`time_type`;
     * +------------+--------------+------+-----+-------------------+-------+
     * | Field      | Type         | Null | Key | Default           | Extra |
     * +------------+--------------+------+-----+-------------------+-------+
     * | date       | date         | YES  |     | NULL              |       |
     * | time       | time         | YES  |     | NULL              |       |
     * | time6      | time(6)      | YES  |     | NULL              |       |
     * | datetime   | datetime     | YES  |     | CURRENT_TIMESTAMP |       |
     * | datetime6  | datetime(6)  | YES  |     | NULL              |       |
     * | timestamp  | timestamp    | NO   |     | CURRENT_TIMESTAMP |       |
     * | timestamp6 | timestamp(6) | YES  |     | NULL              |       |
     * | year       | year(4)      | YES  |     | NULL              |       |
     * | year4      | year(4)      | YES  |     | NULL              |       |
     * +------------+--------------+------+-----+-------------------+-------+
     * 9 rows in set (0.00 sec)
     */
    private List<TableMapLogEvent.Column> mockTimeTypeTable() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column date = new TableMapLogEvent.Column("date", true, "date", null, null, null, null, null, null, "date", null, null, "NULL");
        TableMapLogEvent.Column time = new TableMapLogEvent.Column("time", true, "time", null, null, null, "0", null, null, "time", null, null, "NULL");
        TableMapLogEvent.Column time6 = new TableMapLogEvent.Column("time6", true, "time", null, null, null, "6", null, null, "time(6)", null, null, "NULL");
        TableMapLogEvent.Column datetime = new TableMapLogEvent.Column("datetime", true, "datetime", null, null, null, "0", null, null, "datetime", null, null, "NULL");
        TableMapLogEvent.Column datetime6 = new TableMapLogEvent.Column("datetime6", true, "datetime", null, null, null, "6", null, null, "datetime(6)", null, null, "NULL");
        TableMapLogEvent.Column timestamp = new TableMapLogEvent.Column("timestamp", false, "timestamp", null, null, null, "0", null, null, "timestamp", null, null, "NULL");
        TableMapLogEvent.Column timestamp6 = new TableMapLogEvent.Column("timestamp6", true, "timestamp", null, null, null, "6", null, null, "timestamp(6)", null, null, "NULL");
        TableMapLogEvent.Column year = new TableMapLogEvent.Column("year", true, "year", null, null, null, null, null, null, "year(4)", null, null, "NULL");
        TableMapLogEvent.Column year4 = new TableMapLogEvent.Column("year4", true, "year", null, null, null, null, null, null, "year(4)", null, null, "NULL");

        columns.add(date);
        columns.add(time);
        columns.add(time6);
        columns.add(datetime);
        columns.add(datetime6);
        columns.add(timestamp);
        columns.add(timestamp6);
        columns.add(year);
        columns.add(year4);

        return columns;
    }


    private ByteBuf initTimeTypeBoundaryByteBuf() {
//        String hexString =
//                "73 c6 a6 5d 1e 01 00 00  00 4c 00 00 00 1e 79 00" +
//                        "00 00 00 df 04 00 00 00  00 01 00 02 00 09 ff ff" +
//                        "00 fe 50 c7 0f 80 f3 d0  80 f3 cf 0a 2c 2a 99 a4" +
//                        "60 f3 d0 99 a4 60 f3 cf  0a 2c 2a 5d a6 c3 84 5d" +
//                        "a6 c3 83 0a 2c 2a 77 77  c4 82 00 15";

//        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
//        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
//        byteBuf.writeBytes(bytes);
//
//        return byteBuf;
        return null;
    }

    /**
     * mysql> desc `drc4`.`time_type_boundary`;                                                                                                                                                                                   +----------------+--------------+------+-----+---------+-------+
     * | Field          | Type         | Null | Key | Default | Extra |
     * +----------------+--------------+------+-----+---------+-------+
     * | date_min       | date         | YES  |     | NULL    |       |
     * | date_max       | date         | YES  |     | NULL    |       |
     * | time_min       | time         | YES  |     | NULL    |       |
     * | time_max       | time         | YES  |     | NULL    |       |
     * | time1          | time(1)      | YES  |     | NULL    |       |
     * | time3          | time(3)      | YES  |     | NULL    |       |
     * | time5          | time(5)      | YES  |     | NULL    |       |
     * | time6_min      | time(6)      | YES  |     | NULL    |       |
     * | time6_max      | time(6)      | YES  |     | NULL    |       |
     * | datetime_min   | datetime     | YES  |     | NULL    |       |
     * | datetime_max   | datetime     | YES  |     | NULL    |       |
     * | datetime1      | datetime(1)  | YES  |     | NULL    |       |
     * | datetime3      | datetime(3)  | YES  |     | NULL    |       |
     * | datetime5      | datetime(5)  | YES  |     | NULL    |       |
     * | datetime6_min  | datetime(6)  | YES  |     | NULL    |       |
     * | datetime6_max  | datetime(6)  | YES  |     | NULL    |       |
     * | timestamp_min  | timestamp    | YES  |     | NULL    |       |
     * | timestamp_max  | timestamp    | YES  |     | NULL    |       |
     * | timestamp1     | timestamp(1) | YES  |     | NULL    |       |
     * | timestamp3     | timestamp(3) | YES  |     | NULL    |       |
     * | timestamp5     | timestamp(5) | YES  |     | NULL    |       |
     * | timestamp6_min | timestamp(6) | YES  |     | NULL    |       |
     * | timestamp6_max | timestamp(6) | YES  |     | NULL    |       |
     * | year_min       | year(4)      | YES  |     | NULL    |       |
     * | year_max       | year(4)      | YES  |     | NULL    |       |
     * +----------------+--------------+------+-----+---------+-------+
     * 25 rows in set (0.00 sec)
     */
    private List<TableMapLogEvent.Column> mockTimeTypeBoundaryTable() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
//        TableMapLogEvent.Column date = new TableMapLogEvent.Column("date", true, "date", null, null, null, null, null, null, "date", null, null);
//        TableMapLogEvent.Column time = new TableMapLogEvent.Column("time", true, "time", null, null, null, "0", null, null, "time", null, null);
//        TableMapLogEvent.Column time6 = new TableMapLogEvent.Column("time6", true, "time", null, null, null, "6", null, null, "time(6)", null, null);
//        TableMapLogEvent.Column datetime = new TableMapLogEvent.Column("datetime", true, "datetime", null, null, null, "0", null, null, "datetime", null, null);
//        TableMapLogEvent.Column datetime6 = new TableMapLogEvent.Column("datetime6", true, "datetime", null, null, null, "6", null, null, "datetime(6)", null, null);
//        TableMapLogEvent.Column timestamp = new TableMapLogEvent.Column("timestamp", false, "timestamp", null, null, null, "0", null, null, "timestamp", null, null);
//        TableMapLogEvent.Column timestamp6 = new TableMapLogEvent.Column("timestamp6", true, "timestamp", null, null, null, "6", null, null, "timestamp(6)", null, null);
//        TableMapLogEvent.Column year = new TableMapLogEvent.Column("year", true, "year", null, null, null, null, null, null, "year(4)", null, null);
//        TableMapLogEvent.Column year4 = new TableMapLogEvent.Column("year4", true, "year", null, null, null, null, null, null, "year(4)", null, null);
//
//        columns.add(date);
//        columns.add(time);
//        columns.add(time6);
//        columns.add(datetime);
//        columns.add(datetime6);
//        columns.add(timestamp);
//        columns.add(timestamp6);
//        columns.add(year);
//        columns.add(year4);

        return columns;
    }


    @Test
    public void t() {
        System.out.println( ZonedDateTime.now(ZoneId.of("America/New_York")).toString());
    }
}
