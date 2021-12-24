package com.ctrip.framework.drc.console.local;

import org.junit.Test;
import org.springframework.util.DigestUtils;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-10-21
 */
public class DigestUtilsTest {
    @Test
    public void getAppSignAndTimestamp() {
        long timestamp = System.currentTimeMillis();
        String data = "{\"clusterName\":\"bbzdrcbenchmarkdb_dalcluster\",\"failoverGroups\":[\"fat-fx-drc1\"],\"recoverGroups\":[\"fat-fx-drc2\"],\"groups\":[{\"name\":\"fat-fx-drc1\",\"down\":true,\"nodes\":[\"10.2.72.230:55111\",\"10.2.72.247:55111\"]},{\"name\":\"fat-fx-drc2\",\"down\":false,\"nodes\":[\"10.2.72.246:55111\",\"10.2.72.248:55111\"]}],\"isForced\":false}";
        String token = "beacon";
        String generatedAppSign = DigestUtils.md5DigestAsHex((timestamp + data + token).getBytes()).toUpperCase();
        System.out.println("timestamp: " + timestamp);
        System.out.println("generatedAppSign: " + generatedAppSign);
    }
}
