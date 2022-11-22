package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.mq.MessengerProperties;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jixinwang on 2022/10/17
 */
public class MessengerPropertiesTest {

    private String config = "{\n" +
            "        \"mqConfigs\": [\n" +
            "            {\n" +
            "                \"mqType\": \"qmq\",\n" +
            "                \"table\": \"d1\",\n" +
            "                \"topic\": \"topicName\",\n" +
            "                \"serialization\": \"arvo\",\n" +
            "                \"persistent\": true,\n" +
            "                \"persistentDb\": \"db1\",\n" +
            "                \"order\": true,\n" +
            "                \"orderKey\": \"key1\",\n" +
            "                \"delayTime\": 123,\n" +
            "                \"processor\": \"java\"\n" +
            "            }\n" +
            "        ]\n" +
            "    }";

    @Test
    public void from() {

        MessengerProperties messengerProperties = null;
        try {
            messengerProperties = MessengerProperties.from(config);
            Assert.assertTrue(messengerProperties.getMqConfigs().get(0).isOrder());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }
}
