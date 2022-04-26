package com.ctrip.framework.drc.core.server.utils;

import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class RowsEventUtils {

    /**
     * transform outside load() for multi rows event
     */
    public static void transformMetaAndType(Columns originColumns, Columns columns) {
        for (int i = 0; i < originColumns.size(); ++i) {
            MysqlFieldType type = MysqlFieldType.getMysqlFieldType(originColumns.get(i).getType());
            if (MysqlFieldType.mysql_type_string.equals(type)) {
                int meta = originColumns.get(i).getMeta();
                if (meta >= 256) {
                    final Pair<Integer, Integer> realMetaAndType = getRealMetaAndType(meta);
                    // meta
                    columns.get(i).setMeta(realMetaAndType.getKey());

                    // type
                    int typeInt = realMetaAndType.getValue();
                    if (!type.equals(MysqlFieldType.mysql_type_set)
                            && !type.equals(MysqlFieldType.mysql_type_enum)
                            && !type.equals(MysqlFieldType.mysql_type_string)) {
                        throw new IllegalStateException("MySQL binlog string type can only be converted into string, enum, set types.");
                    }
                    columns.get(i).setType(typeInt);
                }
            } else {
                columns.get(i).setMeta(originColumns.get(i).getMeta());  //update meta
                columns.get(i).setType(originColumns.get(i).getType());  //update type
            }
        }
    }

    public static Pair<Integer, Integer> getRealMetaAndType(final int meta) {
        int byte0 = meta >> 8;
        int byte1 = meta & 0xff;
        if ((byte0 & 0x30) != 0x30) { // 0x30 = 0011 0000
            return new Pair<>(
                    byte1 | (((byte0 & 0x30) ^ 0x30) << 4),
                    byte0 | 0x30
            );
        } else {
            return new Pair<>(byte1, byte0);
        }
    }
}
