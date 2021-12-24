package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;

/**
 * Created by mingdongli
 * 2019/9/28 下午3:59.
 */
public class ColumnsQueryCommandExecutor extends QueryCommandExecutor {

    public ColumnsQueryCommandExecutor(CommandHandler commandHandler) {
        super(commandHandler);
    }

    /**
     * 查表结构
     * mysql> SELECT * FROM information_schema.columns WHERE table_name = 'charset_type'\G;                                                                                                                                       *************************** 1. row ***************************
     *       0     TABLE_CATALOG: def
     *       1      TABLE_SCHEMA: drc4
     *       2        TABLE_NAME: charset_type
     *       3       COLUMN_NAME: varchar4000
     *       4  ORDINAL_POSITION: 1
     *       5    COLUMN_DEFAULT: NULL
     *       6       IS_NULLABLE: YES
     *       7         DATA_TYPE: varchar
     *8 CHARACTER_MAXIMUM_LENGTH: 1000
     * 9  CHARACTER_OCTET_LENGTH: 4000
     *    10    NUMERIC_PRECISION: NULL
     *    11        NUMERIC_SCALE: NULL
     *    12   DATETIME_PRECISION: NULL
     *    13   CHARACTER_SET_NAME: utf8mb4
     *    14       COLLATION_NAME: utf8mb4_general_ci
     *    15          COLUMN_TYPE: varchar(1000)
     *    16           COLUMN_KEY:
     *    17                EXTRA:
     *    18           PRIVILEGES: select,insert,update,references
     *    19       COLUMN_COMMENT:
     *   20 GENERATION_EXPRESSION:
     */
    @Override
    protected String getQueryString() {
        return "SELECT * FROM information_schema.columns WHERE table_schema NOT IN  ('information_schema','mysql','performance_schema','sys') ORDER BY table_schema;";
    }
}
