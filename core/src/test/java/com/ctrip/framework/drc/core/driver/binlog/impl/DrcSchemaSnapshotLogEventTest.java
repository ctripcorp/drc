package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/3/9
 */
public class DrcSchemaSnapshotLogEventTest {

    private Map<String, Map<String, String>> ddls = Maps.newHashMap();

    private static final String DB1 = "drc1";

    private static final String TABLE1 = "insert1";

    private static final String DB1_TABLE1 = "CREATE TABLE `drc1`.`insert1` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private static final String TABLE2 = "multi_type_number";

    private static final String DB1_TABLE2 = "CREATE TABLE `drc1`.`multi_type_number` (\n" +
            "  `bit1` bit(8) unique,\n" +
            "  `bit2` bit(16),\n" +
            "  `bit3` bit(24),\n" +
            "  `bit4` bit(32),\n" +
            "  `bit5` bit(40),\n" +
            "  `bit6` bit(48),\n" +
            "  `bit7` bit(56),\n" +
            "  `bit8` bit(64),\n" +
            "  `tinyint` tinyint(5),\n" +
            "  `smallint` smallint(10),\n" +
            "  `mediumint` mediumint(15),\n" +
            "  `int` int(20),\n" +
            "  `integer` int(20),\n" +
            "  `bigint` bigint(100),\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'\n" +
            ") ENGINE=InnoDB;";

    private static final String DB2 = "drc2";

    private static final String TABLE3 = "multi_type";

    private static final String DB2_TABLE3 = "CREATE TABLE `drc2`.`multi_type` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private static final String TABLE4 = "charset_type";

    private static final String DB2_TABLE4 = "CREATE TABLE `drc2`.`charset_type` (\n" +
            "  `varchar4000` varchar(1000) CHARACTER SET utf8mb4,\n" +
            "  `char1000` char(250) CHARACTER SET utf8mb4,\n" +
            "  `varbinary1800` varbinary(1800),\n" +
            "  `binary200` binary(200),\n" +
            "  `tinyblob` tinyblob,\n" +
            "  `mediumblob` mediumblob,\n" +
            "  `blob` blob,\n" +
            "  `longblob` longblob,\n" +
            "  `tinytext` tinytext CHARACTER SET utf8mb4,\n" +
            "  `mediumtext` mediumtext CHARACTER SET utf8mb4,\n" +
            "  `text` text CHARACTER SET utf8mb4,\n" +
            "  `longtext` longtext CHARACTER SET utf8mb4,\n" +
            "  `longtextwithoutcharset` longtext CHARACTER SET latin1,\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB;";

    private DrcSchemaSnapshotLogEvent schemaSnapshotLogEvent;

    @Before
    public void setUp() throws Exception {
        Map<String, String> table1 = Maps.newHashMap();
        table1.put(TABLE1, DB1_TABLE1);
        table1.put(TABLE2, DB1_TABLE2);
        ddls.put(DB1, table1);

        Map<String, String> table2 = Maps.newHashMap();
        table2.put(TABLE3, DB2_TABLE3);
        table2.put(TABLE4, DB2_TABLE4);
        ddls.put(DB2, table2);

        schemaSnapshotLogEvent = new DrcSchemaSnapshotLogEvent(ddls, 0 , 10);
    }

    @Test
    public void read() {
        ByteBuf headByteBuf = schemaSnapshotLogEvent.getLogEventHeader().getHeaderBuf();
        headByteBuf.readerIndex(0);

        ByteBuf payloadByteBuf =  schemaSnapshotLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);

        DrcSchemaSnapshotLogEvent clone = new DrcSchemaSnapshotLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headByteBuf.readableBytes() + payloadByteBuf.readableBytes());
        compositeByteBuf.addComponents(true, headByteBuf, payloadByteBuf);

        DrcSchemaSnapshotLogEvent drcSchemaSnapshotLogEvent = (DrcSchemaSnapshotLogEvent) clone.read(compositeByteBuf);

        Map<String, Map<String, String>> cloneDdls = drcSchemaSnapshotLogEvent.getDdls();
        for (Map.Entry<String, Map<String, String>> entry : ddls.entrySet()) {
            String dbName = entry.getKey();
            Map<String, String> cloneTables = cloneDdls.get(dbName);
            Map<String, String> originTables = ddls.get(dbName);
            Assert.assertEquals(originTables.size(), cloneTables.size());

            for (Map.Entry<String, String> tables: originTables.entrySet()) {
                String table = tables.getKey();
                Assert.assertEquals(tables.getValue(), cloneTables.get(table));
            }
        }
    }

}