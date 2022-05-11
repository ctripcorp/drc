package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.AllTests.ROW_FILTER_PROPERTIES;
import static com.ctrip.framework.drc.core.meta.DataMediaConfig.from;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * Created by mingdongli
 * 2019/9/18 下午2:40.
 */
public abstract class AbstractEventTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String schemaName = "drc1";

    protected static final String tableName = "insert1";

    protected static final String GTID = "a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad:66";

    protected DataMediaConfig dataMediaConfig;

    protected TableMapLogEvent tableMapLogEvent;

    protected TableMapLogEvent drcTableMapLogEvent;

    protected WriteRowsEvent writeRowsEvent;

    public static List<List<Object>> result = Lists.newArrayList();

    @Before
    public void setUp() throws Exception {
        if (result.isEmpty()) {
            result.add(Lists.newArrayList("1", "2", "3", "4"));
        }
        dataMediaConfig = from("registryKey", String.format(getProperties(), getRowsFilterType().getName(), getLocations()));
        ByteBuf tByteBuf = tableMapEvent();
        tableMapLogEvent = new TableMapLogEvent().read(tByteBuf);
        ByteBuf wByteBuf = writeRowsEvent();
        writeRowsEvent = new WriteRowsEvent().read(wByteBuf);
        drcTableMapLogEvent = drcTableMapEvent();

        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        Columns columns = Columns.from(drcTableMapLogEvent.getColumns());
        transformMetaAndType(originColumns, columns);
        writeRowsEvent.load(columns);
    }

    protected abstract RowsFilterType getRowsFilterType();

    protected String getProperties() {
        return ROW_FILTER_PROPERTIES;
    }

    protected String getLocations() {
        return "SG,FRA";
    }

    /*
    *  SELECT COLUMN_NAME,IS_NULLABLE,DATA_TYPE,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_DEFAULT FROM information_schema.columns WHERE TABLE_NAME IN  ('insert1') and TABLE_SCHEMA in ("drc1") ORDER BY table_schema;
    *
    *  CREATE TABLE `insert1` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `one` varchar(30) DEFAULT 'one',
            `two` varchar(1000) DEFAULT 'two',
            `three` char(30) DEFAULT NULL,
            `four` char(255) DEFAULT NULL,
            `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    *
    */
    private ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
            new TableMapLogEvent.Column("id", false, "int", null, "10", "0", null, null, null, "int(11)", "PRI", "auto_increment", null),
            new TableMapLogEvent.Column("one", true, "varchar", "90", null, null, null, "utf8", "utf8_general_ci", "varchar(30)", null, null, "one"),
            new TableMapLogEvent.Column("two", true, "varchar", "3000", null, null, null, "utf8", "utf8_general_ci", "varchar(1000)", null, null, "two"),
            new TableMapLogEvent.Column("three", true, "char", "90", null, null, null, "utf8", "utf8_general_ci", "char(30)", null, null, null),
            new TableMapLogEvent.Column("four", true, "char", "765", null, null, null, "utf8", "utf8_general_ci", "char(255)", null, null, null),
            new TableMapLogEvent.Column("datachange_lasttime", false, "timestamp", null, null, null, "3", null, null, "timestamp(3)", null, "on update CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)")
    );
    private List<List<String>> identifiers = Lists.<List<String>>newArrayList(
            Lists.<String>newArrayList("id")
    );

    protected TableMapLogEvent drcTableMapEvent() throws IOException {
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }

    protected ByteBuf tableMapEvent() {
        String hexString = "45 8d 67 62 13 64 00 00 00 40 00 00 00 58 53 00 00 00 00" +
                "6c 00 00 00 00 00 01 00 04 64 72 63 31 00 07 69" +
                "6e 73 65 72 74 31 00 06 03 0f 0f fe fe 11 09 5a" +
                "00 b8 0b fe 5a de fd 03 1e 01 c4 10 4b";
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /*
     * insert into insert1(`three`,`four`) values("1","2"),("3","4"),("5","6");
     */
    protected ByteBuf writeRowsEvent() {
        String hexString = "45 8d 67 62 1e 64 00 00 00 6e 00 00 00 c6 53 00 00 00 00" +
                "6c 00 00 00 00 00 01 00 02 00 06 ff c0 12 00 00" +
                "00 03 6f 6e 65 03 00 74 77 6f 01 31 01 00 32 62" +
                "67 8d 45 15 ea c0 14 00 00 00 03 6f 6e 65 03 00" +
                "74 77 6f 01 33 01 00 34 62 67 8d 45 15 ea c0 16" +
                "00 00 00 03 6f 6e 65 03 00 74 77 6f 01 35 01 00" +
                "36 62 67 8d 45 15 ea f6 b7 7d bb";
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    protected ByteBuf getGtidEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    protected ByteBuf getXidEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x10, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf5, (byte) 0x2f, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x44, (byte) 0x04, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x82, (byte) 0xe5, (byte) 0xc7, (byte) 0x3a
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    public static byte[] toBytesFromHexString(String hexString) {
        // mac control+command+G multi rows operation
        hexString = hexString.replaceAll(" ", "");
        hexString = hexString.replaceAll("\n", "");
        return HexUtils.fromHexString(hexString);
    }

}
