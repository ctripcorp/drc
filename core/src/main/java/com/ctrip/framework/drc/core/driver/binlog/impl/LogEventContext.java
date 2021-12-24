package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Map;

/**
 * Created by @author zhuYongMing on 2019/9/16.
 */
public class LogEventContext {

    private static LogEventContext logEventContext;

    private final Map<Long, TableMapLogEvent> tables = Maps.newConcurrentMap(); // <tableId, tableMapEvent>

    public static LogEventContext getInstant() {
        if (null == logEventContext) {
            synchronized (LogEventContext.class) {
                if (null == logEventContext) {
                    logEventContext = new LogEventContext();
                }
            }
        }

        return logEventContext;
    }

    private LogEventContext() {
        // mock tableMapLogEvent
//        final TableMapLogEvent rowImage3 = new TableMapLogEvent(false).read(mockRowImage3ByteBuf());
//        final TableMapLogEvent perconaDefaultValue2 = new TableMapLogEvent(false).read(mockPerconaDefaultValue2ByteBuf());
//        tables.put(rowImage3.getTableId(), rowImage3);
//        tables.put(perconaDefaultValue2.getTableId(), perconaDefaultValue2);
    }


    public void putTableMapLogEvent(final TableMapLogEvent tableMapLogEvent) {
        tables.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
    }

    public TableMapLogEvent getTableMapLogEvent(final Long tableId) {
        return tables.get(tableId);
    }

    /**
     * no extra data
     *
     * mysql> desc row_image3;
     * +---------+-------------+------+-----+---------+-------+
     * | Field   | Type        | Null | Key | Default | Extra |
     * +---------+-------------+------+-----+---------+-------+
     * | id      | int(11)     | YES  |     | NULL    |       |
     * | name    | char(10)    | YES  |     | NULL    |       |
     * | age     | int(10)     | YES  |     | NULL    |       |
     * | varchar | varchar(86) | YES  |     | NULL    |       |
     * +---------+-------------+------+-----+---------+-------+
     *
     * # at 813
     * #190914 20:56:16 server id 1  end_log_pos 878 CRC32 0x4d7a508e  Table_map: `gtid_test`.`row_image3` mapped to number 123
     *
     * 0000032d  70 e3 7c 5d 13 01 00 00  00 41 00 00 00 6e 03 00  |p.|].....A...n..|
     * 0000033d  00 00 00 7b 00 00 00 00  00 01 00 09 67 74 69 64  |...{........gtid|
     * 0000034d  5f 74 65 73 74 00 0a 72  6f 77 5f 69 6d 61 67 65  |_test..row_image|
     * 0000035d  33 00 04 03 fe 03 0f 04  fe 1e 02 01 0f 8e 50 7a 4d
     */
    private ByteBuf mockRowImage3ByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[] {
                (byte) 0x70, (byte) 0xe3, (byte) 0x7c, (byte) 0x5d, (byte) 0x13, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6e, (byte) 0x03, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x09, (byte) 0x67, (byte) 0x74, (byte) 0x69, (byte) 0x64,

                (byte) 0x5f, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x00, (byte) 0x0a, (byte) 0x72,
                (byte) 0x6f, (byte) 0x77, (byte) 0x5f, (byte) 0x69, (byte) 0x6d, (byte) 0x61, (byte) 0x67, (byte) 0x65,

                (byte) 0x33, (byte) 0x00, (byte) 0x04, (byte) 0x03, (byte) 0xfe, (byte) 0x03, (byte) 0x0f, (byte) 0x04,
                (byte) 0xfe, (byte) 0x1e, (byte) 0x02, (byte) 0x01, (byte) 0x0f, (byte) 0x8e, (byte) 0x50, (byte) 0x7a,

                (byte) 0x4d
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     * table:
     * CREATE TABLE `percona`.`default_value2` (
     * 	`id` int(11) NOT NULL AUTO_INCREMENT,
     * 	`first` varchar(30) DEFAULT "",
     * 	`second` int(20) DEFAULT 0,
     * 	`three` varchar(30) DEFAULT "three",
     * 	`four` int(20),
     * 	PRIMARY KEY (`id`)
     * ) COMMENT='';
     *
     * binlog:
     * # at 774
     * #190926 19:36:15 server id 1  end_log_pos 842 CRC32 0x5336b50b  Table_map: `percona`.`default_value2` mapped to number 108
     *
     * binary:
     * 00000306  af a2 8c 5d 13 01 00 00  00 44 00 00 00 4a 03 00  |...].....D...J..|
     * 00000316  00 00 00 6c 00 00 00 00  00 01 00 07 70 65 72 63  |...l........perc|
     * 00000326  6f 6e 61 00 0e 64 65 66  61 75 6c 74 5f 76 61 6c  |ona..default_val|
     * 00000336  75 65 32 00 05 03 0f 03  0f 03 04 1e 00 1e 00 1e  |ue2.............|
     * 00000346  0b b5 36 53
     */
    private ByteBuf mockPerconaDefaultValue2ByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(67);
        byte[] bytes = new byte[] {
                (byte) 0xaf, (byte) 0xa2, (byte) 0x8c, (byte) 0x5d, (byte) 0x13, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x44, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x4a, (byte) 0x03, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6c, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x07, (byte) 0x70, (byte) 0x65, (byte) 0x72, (byte) 0x63,

                (byte) 0x6f, (byte) 0x6e, (byte) 0x61, (byte) 0x00, (byte) 0x0e, (byte) 0x64, (byte) 0x65, (byte) 0x66,
                (byte) 0x61, (byte) 0x75, (byte) 0x6c, (byte) 0x74, (byte) 0x5f, (byte) 0x76, (byte) 0x61, (byte) 0x6c,

                (byte) 0x75, (byte) 0x65, (byte) 0x32, (byte) 0x00, (byte) 0x05, (byte) 0x03, (byte) 0x0f, (byte) 0x03,
                (byte) 0x0f, (byte) 0x03, (byte) 0x04, (byte) 0x1e, (byte) 0x00, (byte) 0x1e, (byte) 0x00, (byte) 0x1e,

                (byte) 0x0b, (byte) 0xb5, (byte) 0x36, (byte) 0x53
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
