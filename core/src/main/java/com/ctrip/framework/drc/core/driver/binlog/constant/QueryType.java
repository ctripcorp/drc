package com.ctrip.framework.drc.core.driver.binlog.constant;

import com.google.common.collect.Sets;

import java.util.Set;

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
    private static final Set<QueryType> DML_TYPE = Sets.newHashSet(INSERT, UPDATE, DELETE, TRUNCATE);

    public static QueryType getQueryType(int type) {
        for(QueryType queryType : values()) {
            if (queryType.ordinal() == type) {
                return queryType;
            }
        }

        throw new IllegalArgumentException("not support for type " + type);
    }


    public boolean isDmlOrTruncate() {
        return DML_TYPE.contains(this);
    }
}
