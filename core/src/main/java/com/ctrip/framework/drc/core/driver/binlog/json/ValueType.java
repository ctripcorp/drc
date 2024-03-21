package com.ctrip.framework.drc.core.driver.binlog.json;

/**
 * @ClassName ValueType
 * @Author haodongPan
 * @Date 2024/2/5 19:43
 * @Version: $
 */

import java.util.HashMap;
import java.util.Map;

/**
 * The set of values that can be used within a MySQL JSON value.
 * <p>
 * These values are defined in the MySQL codebase in the
 * <a href="https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h">json_binary.h</a> file, and are:
 *
 * <pre>
 *   type ::=
 *       0x00 |  // small JSON object
 *       0x01 |  // large JSON object
 *       0x02 |  // small JSON array
 *       0x03 |  // large JSON array
 *       0x04 |  // literal (true/false/null)
 *       0x05 |  // int16
 *       0x06 |  // uint16
 *       0x07 |  // int32
 *       0x08 |  // uint32
 *       0x09 |  // int64
 *       0x0a |  // uint64
 *       0x0b |  // double
 *       0x0c |  // utf8mb4 string
 *       0x0f    // custom data (any MySQL data type)
 * </pre>
 *
 * @author <a href="mailto:rhauch@gmail.com">Randall Hauch</a>
 */
public enum ValueType {

    SMALL_OBJECT(0x00),
    LARGE_OBJECT(0x01),
    SMALL_ARRAY(0x02),
    LARGE_ARRAY(0x03),
    LITERAL(0x04),
    INT16(0x05),
    UINT16(0x06),
    INT32(0x07),
    UINT32(0x08),
    INT64(0x09),
    UINT64(0x0a),
    DOUBLE(0x0b),
    STRING(0x0c),
    OPAQUE(0x0f);

    private final int code;

    ValueType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    private static final Map<Integer, ValueType> TYPE_BY_CODE;

    static {
        TYPE_BY_CODE = new HashMap<Integer, ValueType>();
        for (ValueType type : values()) {
            TYPE_BY_CODE.put(type.code, type);
        }
    }

    public static ValueType byCode(int code) {
        return TYPE_BY_CODE.get(code);
    }

}
