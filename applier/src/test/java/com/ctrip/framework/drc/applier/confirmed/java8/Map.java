package com.ctrip.framework.drc.applier.confirmed.java8;

import org.junit.Test;

import java.util.HashMap;

/**
 * @Author Slight
 * Aug 19, 2020
 */
public class Map {

    @Test
    public void computeIfAbsent() {
        HashMap<String, String> map = new HashMap<>();
        String value1 = map.computeIfAbsent("key1", k -> "value1");
        assert value1 == "value1";
        assert map.get("key1") == "value1";
        String value2 = map.computeIfAbsent("key1", k -> "value2");
        assert value2 == "value1";
        assert map.get("key1") == "value1";
    }
}
