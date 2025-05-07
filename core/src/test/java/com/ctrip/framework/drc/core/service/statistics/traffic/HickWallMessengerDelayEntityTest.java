package com.ctrip.framework.drc.core.service.statistics.traffic;

import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.util.ClassUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author yongnian
 * @create 2025/1/13 16:21
 */
public class HickWallMessengerDelayEntityTest {
    @Test
    public void name() throws IOException {
        String fileName = ClassUtils.getDefaultClassLoader().getResource("hickwall_mq_delay.json").getPath();
        String jsonString = FileUtils.readFileToString(new File(fileName));
        Assert.assertNotNull(jsonString);

        Map<String, HickWallMessengerDelayEntity> qmqMap = HickWallMessengerDelayEntity.parseJson(jsonString, MqType.qmq);
        Assert.assertEquals(3, qmqMap.size());
        Assert.assertEquals(594, (long) qmqMap.get("fat-bbz").getDelay());
        Assert.assertEquals(25, (long) qmqMap.get("fat-flt").getDelay());
        Assert.assertEquals(596, (long) qmqMap.get("fat-htl2").getDelay());


        Map<String, HickWallMessengerDelayEntity> kafkaMap = HickWallMessengerDelayEntity.parseJson(jsonString, MqType.kafka);
        Assert.assertEquals(1, kafkaMap.size());
        Assert.assertEquals(1600, (long) kafkaMap.get("fat-bbz").getDelay());

    }
}

