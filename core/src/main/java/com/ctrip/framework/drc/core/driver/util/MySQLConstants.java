package com.ctrip.framework.drc.core.driver.util;

import com.google.common.collect.Sets;

import java.util.HashSet;

/**
 * Created by mingdongli
 * 2019/9/6 下午1:09.
 */
public abstract class MySQLConstants {

        public static final int MAX_PACKET_LENGTH = (1 << 30);  //3个字节24位

        public static final int HEADER_PACKET_LENGTH_FIELD_LENGTH = 3;

        public static final int HEADER_PACKET_LENGTH_FIELD_OFFSET = 0;

        public static final int HEADER_PACKET_LENGTH = 4;

        public static final int HEADER_PACKET_NUMBER_FIELD_LENGTH = 1;

        public static final byte NULL_TERMINATED_STRING_DELIMITER = 0x00;

        public static final byte DEFAULT_PROTOCOL_VERSION = 0x0a;

        public static final int FIELD_COUNT_FIELD_LENGTH = 1;

        public static final int EVENT_TYPE_OFFSET = 4;

        public static final int EVENT_LEN_OFFSET = 9;

        public static final int EVENT_LEN_LENTH = 4;

        public static final long DEFAULT_BINLOG_FILE_START_POSITION = 4;

        public static final int BINLOG_TRANSACTION_OFFSET_LENGTH = 4;

        public static final int BINLOG_CHECKSUM_LENGTH = 4;


        //mysql command
        public static final String SHOW_DATABASES_QUERY = "SHOW DATABASES;";

        public static final String DROP_DATABASE = "DROP DATABASE %s;";

        public static HashSet<String> EXCLUDED_DB = Sets.newHashSet("configdb", "mysql", "performance_schema", "sys", "information_schema");

}
