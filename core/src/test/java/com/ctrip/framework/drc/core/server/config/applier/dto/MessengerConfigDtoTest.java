package com.ctrip.framework.drc.core.server.config.applier.dto;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.mq.MqType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author yongnian
 * @create 2025/1/10 12:31
 */
public class MessengerConfigDtoTest {
    @Test
    public void testGetRegistryKey() {
        MessengerConfigDto configDto = new MessengerConfigDto();
        DBInfo target = new DBInfo();
        target.mhaName = "srcMha";
        configDto.cluster = "srcMha_dalcluster";
        configDto.setTarget(target);
        InstanceInfo replicator = new InstanceInfo();
        replicator.mhaName = MqType.qmq.getRegistryKeySuffix();
        configDto.setReplicator(replicator);
        Assert.assertEquals("srcMha_dalcluster.srcMha._drc_mq", configDto.getRegistryKey());

        configDto.setIncludedDbs("testDb");
        configDto.setApplyMode(ApplyMode.db_mq.getType());
        Assert.assertEquals("srcMha_dalcluster.srcMha._drc_mq.testDb", configDto.getRegistryKey());
    }
}
