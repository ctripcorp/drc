package com.ctrip.framework.drc.applier.confirmed.java8;

import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class List {

    @Test
    public void testClear() {
        ArrayList<Integer> list = Lists.newArrayList();
        list.add(1);
        list.add(1);
        assertEquals(2, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testSet() {
        ArrayList<Integer> list = Lists.newArrayList();
        list.add(1);
        list.add(2);
        assertEquals(1, (int) list.set(0, 2));
        assertEquals(2, (int) list.set(1, 3));
    }

    @Test (expected = IndexOutOfBoundsException.class)
    public void testSetIndexOutOfBound() {
        ArrayList<Integer> list = Lists.newArrayList();
        list.add(1);
        list.add(2);
        list.set(2, 2);
    }

    @Test (expected = IndexOutOfBoundsException.class)
    public void testAddWithIndex() {
        ArrayList<Integer> list = Lists.newArrayList();
        list.add(0, 1);
        list.add(1, 2);
        list.add(5, 2);
    }

    @Test
    public void testAddWithIndexToArray() {
        Integer[] integers = new Integer[10];
        integers[3] = 1;
        assertNull(integers[0]);
    }
}
