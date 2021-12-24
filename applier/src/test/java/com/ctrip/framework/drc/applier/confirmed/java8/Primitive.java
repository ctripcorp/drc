package com.ctrip.framework.drc.applier.confirmed.java8;

import org.junit.Test;

/**
 * @Author Slight
 * May 14, 2020
 */
public class Primitive {

    @Test
    public void Integer() {
        Object i = 1;
        assert i instanceof Integer;
    }

    @Test
    public void Long() {
        Object l = (long) 1;
        assert l instanceof Long;
    }
}
