package com.ctrip.framework.drc.core.driver.schema.data;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public class KeyTest {

    @Test
    public void simpleUse() {
        TestKey key1 = new TestKey("hello");
        TestKey key2 = new TestKey("hello");
        assertNotSame(key1, key2);
        assertTrue(key1.equals(key2));
        assertFalse(key1.equals(null));
        assertFalse(key1.equals(1));
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    class TestKey extends Key {

        public final String inner;

        public TestKey(String inner) {
            this.inner = inner;
        }

        @Override
        public String toString() {
            return inner;
        }
    }
}