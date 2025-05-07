package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2024/12/18 19:11
 */
public class MqConfigTest {

    @Test
    public void test() {
        String json = "{\"mqType\":\"qmq\",\"serialization\":\"json\",\"persistent\":false,\"order\":true,\"orderKey\":\"key1\",\"delayTime\":0}";
        MqConfig mqConfig = JsonUtils.fromJson(json, MqConfig.class);

        String json1 = JsonUtils.toJson(mqConfig);
        System.out.println(json1);
        Assert.assertEquals(json, json1);
    }

}
