package com.ctrip.framework.drc.core.driver.schema.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public class TableKeyTest {

    TableKey key = TableKey.from("a", "b");

    @Test
    public void getDatabaseName() {
        assertEquals("a", key.getDatabaseName());
    }

    @Test
    public void getTableName() {
        assertEquals("b", key.getTableName());
    }

    @Test
    public void testToString() {
        assertEquals("`a`.`b`", key.toString());
    }

    @Test
    public void testFrom() {
        assertEquals("prod", TableKey.from("`prod`.`hello`").getDatabaseName());
        assertEquals("hello", TableKey.from("`prod`.`hello`").getTableName());
    }

    @Test
    public void testClone() {
        assertEquals(key, key.clone());
        assertNotSame(key, key.clone());
    }
}