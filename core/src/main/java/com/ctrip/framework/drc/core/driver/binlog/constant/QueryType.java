package com.ctrip.framework.drc.core.driver.binlog.constant;

/**
 * @Author limingdong
 * @create 2020/2/24
 */
public enum QueryType {

    CREATE,

    ALTER,

    ERASE,

    QUERY,

    TRUNCATE,

    RENAME,

    CINDEX,

    DINDEX,

    XACOMMIT,

    XAROLLBACK,

    INSERT,

    UPDATE,

    DELETE;

    public static QueryType getQueryType(int type) {
        for(QueryType queryType : values()) {
            if (queryType.ordinal() == type) {
                return queryType;
            }
        }

        throw new IllegalArgumentException("not support for type " + type);
    }

}
