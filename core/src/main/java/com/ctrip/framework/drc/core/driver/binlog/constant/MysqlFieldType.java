package com.ctrip.framework.drc.core.driver.binlog.constant;

/**
 * Created by @author zhuYongMing on 2019/9/15.
 * mysql all field type, see https://dev.mysql.com/doc/refman/5.7/en/integer-types.html,
 * see https://www.jianshu.com/p/b08f848793d4
 * see https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::MYSQL_TYPE_ENUM
 * all type are finished， 30 + 1(json) = 31
 */
public enum MysqlFieldType {

    mysql_type_decimal(0) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_tiny(1) {
        @Override
        public String[] getLiterals() {
            return new String[]{"tinyint"};
        }
    },
    mysql_type_short(2) {
        @Override
        public String[] getLiterals() {
            return new String[]{"smallint"};
        }
    },
    mysql_type_long(3) {
        @Override
        public String[] getLiterals() {
            // if create table, some column defined integer, in the fact, this column type is int
            return new String[]{"int"};
        }
    },
    mysql_type_float(4) {
        @Override
        public String[] getLiterals() {
            return new String[]{"float"};
        }
    },
    mysql_type_double(5) {
        @Override
        public String[] getLiterals() {
            // if create table, some column defined real, in the fact, this column type is double
            return new String[]{"double"};
        }
    },
    mysql_type_null(6) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_timestamp(7) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_longlong(8) {
        @Override
        public String[] getLiterals() {
            return new String[]{"bigint"};
        }
    },
    mysql_type_int24(9) {
        @Override
        public String[] getLiterals() {
            return new String[]{"mediumint"};
        }
    },
    mysql_type_date(10) {
        @Override
        public String[] getLiterals() {
            return new String[]{"date"};
        }
    },
    mysql_type_time(11) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_datetime(12) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_year(13) {
        @Override
        public String[] getLiterals() {
            return new String[]{"year"};
        }
    },
    mysql_type_newdate(14) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_varchar(15) {
        @Override
        public String[] getLiterals() {
            return new String[]{"varchar", "varbinary"};
        }
    },
    mysql_type_bit(16) {
        @Override
        public String[] getLiterals() {
            return new String[]{"bit"};
        }
    },

    // v2 see http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
    mysql_type_timestamp2(17) {
        @Override
        public String[] getLiterals() {
            return new String[]{"timestamp"};
        }
    },
    mysql_type_datetime2(18) {
        @Override
        public String[] getLiterals() {
            return new String[]{"datetime"};
        }
    },
    mysql_type_time2(19) {
        @Override
        public String[] getLiterals() {
            return new String[]{"time"};
        }
    },
    mysql_type_json(245) {
        @Override
        public String[] getLiterals() {
            return new String[]{"json"};
        }
    },
    mysql_type_newdecimal(246) {
        @Override
        public String[] getLiterals() {
            // if create table, some column defined numeric, in the fact, this column type is decimal
            return new String[]{"decimal"};
        }
    },
    mysql_type_enum(247) {
        @Override
        public String[] getLiterals() {
            return new String[]{"enum"};
        }
    },
    mysql_type_set(248) {
        @Override
        public String[] getLiterals() {
            return new String[]{"set"};
        }
    },
    mysql_type_tiny_blob(249) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_medium_blob(250) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_long_blob(251) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_blob(252) {
        @Override
        public String[] getLiterals() {
            return new String[]{"tinyblob", "mediumblob", "blob", "longblob", "tinytext", "mediumtext", "text", "longtext"};
        }
    },
    mysql_type_var_string(253) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    },
    mysql_type_string(254) {
        @Override
        public String[] getLiterals() {
            return new String[]{"char", "binary"};
        }
    },
    mysql_type_geometry(255) {
        @Override
        public String[] getLiterals() {
            return new String[]{};
        }
    };

    public static MysqlFieldType getMysqlFieldType(final int type) {
        switch (type) {
            case 0:
                return mysql_type_decimal;
            case 1:
                return mysql_type_tiny;
            case 2:
                return mysql_type_short;
            case 3:
                return mysql_type_long;
            case 4:
                return mysql_type_float;
            case 5:
                return mysql_type_double;
            case 6:
                return mysql_type_null;
            case 7:
                return mysql_type_timestamp;
            case 8:
                return mysql_type_longlong;
            case 9:
                return mysql_type_int24;
            case 10:
                return mysql_type_date;
            case 11:
                return mysql_type_time;
            case 12:
                return mysql_type_datetime;
            case 13:
                return mysql_type_year;
            case 14:
                return mysql_type_newdate;
            case 15:
                return mysql_type_varchar;
            case 16:
                return mysql_type_bit;
            case 17:
                return mysql_type_timestamp2;
            case 18:
                return mysql_type_datetime2;
            case 19:
                return mysql_type_time2;
            case 245:
                return mysql_type_json;
            case 246:
                return mysql_type_newdecimal;
            case 247:
                return mysql_type_enum;
            case 248:
                return mysql_type_set;
            case 249:
                return mysql_type_tiny_blob;
            case 250:
                return mysql_type_medium_blob;
            case 251:
                return mysql_type_long_blob;
            case 252:
                return mysql_type_blob;
            case 253:
                return mysql_type_var_string;
            case 254:
                return mysql_type_string;
            case 255:
                return mysql_type_geometry;
            default:
                throw new IllegalStateException(String.format("mysql field type can't match, this mysql field type is %d.", type));
        }
    }

    public static MysqlFieldType getMysqlFieldTypeByLiteral(final String literal) {
        for (MysqlFieldType fieldType : values()) {
            final String[] literals = fieldType.getLiterals();
            for (String l : literals) {
                if (l.equalsIgnoreCase(literal)) {
                    return fieldType;
                }
            }
        }
        throw new UnsupportedOperationException("mysql field not support : " + literal);
    }

    public static boolean isBitType(final String literal) {
        return "bit".equalsIgnoreCase(literal);
    }

    public static boolean isNumberType(final String literal) {
        return "tinyint".equalsIgnoreCase(literal) || "smallint".equalsIgnoreCase(literal)
                || "mediumint".equalsIgnoreCase(literal) || "int".equalsIgnoreCase(literal)
                || "bigint".equalsIgnoreCase(literal);
    }

    public static boolean isBinaryType(final String literal) {
        return "binary".equalsIgnoreCase(literal) || "varbinary".equalsIgnoreCase(literal);
    }


    public static boolean isTextType(final String literal) {
        return "tinytext".equalsIgnoreCase(literal) || "text".equalsIgnoreCase(literal) || "mediumtext".equalsIgnoreCase(literal) || "longtext".equalsIgnoreCase(literal);
    }

    public static boolean is1ByteLengthBlobOrTextType(final String literal) {
        return "tinyblob".equalsIgnoreCase(literal) || "tinytext".equalsIgnoreCase(literal);
    }

    public static boolean is2ByteLengthBlobOrTextType(final String literal) {
        return "blob".equalsIgnoreCase(literal) || "text".equalsIgnoreCase(literal);
    }

    public static boolean is3ByteLengthBlobOrTextType(final String literal) {
        return "mediumblob".equalsIgnoreCase(literal) || "mediumtext".equalsIgnoreCase(literal);
    }

    public static boolean is4ByteLengthBlobOrTextType(final String literal) {
        return "longblob".equalsIgnoreCase(literal) || "longtext".equalsIgnoreCase(literal);
    }

    public static boolean isCharsetType(final String literal) {
        return "char".equalsIgnoreCase(literal) || "varchar".equalsIgnoreCase(literal);
    }

    public static boolean isDatetimePrecisionType(final String literal) {
        return "timestamp".equalsIgnoreCase(literal)
                || "datetime".equalsIgnoreCase(literal)
                || "time".equalsIgnoreCase(literal);
    }

    public static boolean isDatetimePrecisionType(final int type) {
        return mysql_type_timestamp2.type == type
                || mysql_type_datetime2.type == type
                || mysql_type_time2.type == type;
    }


    public static boolean isEnumOrSetType(final int type) {
        return mysql_type_enum.type == type || mysql_type_set.type == type;
    }

    public static boolean isDecimalType(final String literal) {
        return "decimal".equalsIgnoreCase(literal);
    }

    public static boolean isEnumType(final String literal) {
        return "enum".equalsIgnoreCase(literal);
    }

    public static boolean isSetType(final String literal) {
        return "set".equalsIgnoreCase(literal);
    }


    MysqlFieldType(int type) {
        this.type = type;
    }

    public abstract String[] getLiterals();

    private int type;

    public int getType() {
        return type;
    }
}
