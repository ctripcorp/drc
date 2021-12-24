package com.ctrip.framework.drc.applier.confirmed.java8;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Dec 31, 2019
 */
public class StringEqual {

    @Test
    public void testEquals() {
        TableKey key = TableKey.from("abc", "123");
        assertTrue(key.getTableName().equals("123"));
        assertTrue(key.getTableName() == "123");
    }
}
