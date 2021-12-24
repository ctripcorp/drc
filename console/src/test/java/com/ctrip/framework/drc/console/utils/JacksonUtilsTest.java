package com.ctrip.framework.drc.console.utils;


import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-10
 */
public class JacksonUtilsTest {

    ObjectMapper objectMapper = new ObjectMapper();

    private String json = "{\"key1\":\"val1\",\"key2\":\"val2\"}";

    @Test
    public void testGetRootNode(){
        JsonNode root = JacksonUtils.getRootNode("uri", "requestBody", JacksonUtils.HTTP_METHOD_POST);
        Assert.assertNotNull(root);
        System.out.println(root.toString());
        root = JacksonUtils.getRootNode("uri", "requestBody", JacksonUtils.HTTP_METHOD_GET);
        Assert.assertNotNull(root);
        root = JacksonUtils.getRootNode("uri", "requestBody", JacksonUtils.HTTP_METHOD_PUT);
        Assert.assertNotNull(root);
    }

    @Test
    public void getStringObjectMap(){
        try {
            JsonNode root = objectMapper.readTree("{\"fail\": true}");
            Map<String, Object> map = JacksonUtils.getStringObjectMap(root);
            Assert.assertNotNull(map);
            Assert.assertNotEquals(0, map.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetVal() {
        Assert.assertNull(JacksonUtils.getVal(json, "nokey"));
        Assert.assertEquals("val1", JacksonUtils.getVal(json, "key1"));
    }
}
