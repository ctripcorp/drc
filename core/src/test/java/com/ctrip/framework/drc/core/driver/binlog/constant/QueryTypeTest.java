package com.ctrip.framework.drc.core.driver.binlog.constant;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/1/4
 */
public class QueryTypeTest {

    @Test
    public void getQueryType() {
        int typeNum = QueryType.values().length;
        QueryType queryType = QueryType.values()[typeNum - 1];
        QueryType type = QueryType.getQueryType(typeNum - 1);
        assertEquals(queryType, type);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getQueryTypeException() {
        int typeNum = QueryType.values().length;
        QueryType.getQueryType(typeNum);
    }
}