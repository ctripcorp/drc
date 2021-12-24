package com.ctrip.framework.drc.console.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by jixinwang on 2020/6/23
 */
public class JsonUtilsTest {

    @Test
    public void testToJson() {
        Map map = new HashMap();
        map.put("name", "test");
        map.put("value", true);
        String str = JsonUtils.toJson(map);
        assertNotNull(str);
    }

    @Test
    public void testFromJson() {
        Map map = new HashMap();
        map.put("name", "test");
        map.put("value", true);
        String str = JsonUtils.toJson(map);
        Map ret = JsonUtils.fromJson(str, Map.class);
        assertEquals(true, ret.get("value"));
    }

    @Test
    public void testParseObject() {
        Map map = new HashMap();
        map.put("name", "test");
        map.put("value", true);
        String str = JsonUtils.toJson(map);
        JsonObject ret = JsonUtils.parseObject(str);
        assertEquals(true, ret.get("value").getAsBoolean());
    }

    @Test
    public void testParseArray() {
        List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        String listStr = JsonUtils.toJson(list);
        JsonArray jsonArray = JsonUtils.parseArray(listStr);
        assertEquals(2, jsonArray.size());
    }

    @Test
    public void testParse() {
        Map map = new HashMap();
        map.put("name", "test");
        map.put("value", true);
        String str = JsonUtils.toJson(map);
        JsonElement ret = JsonUtils.parse(str);
        assertEquals(true, ret.getAsJsonObject().get("value").getAsBoolean());
    }

    @Test
    public void testFromJsonToList() {
        List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        String listStr = JsonUtils.toJson(list);
        List<String> list1 = JsonUtils.fromJsonToList(listStr, String.class);
        assertEquals(2, list1.size());
    }
}
