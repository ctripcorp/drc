package com.ctrip.framework.drc.core.server.config.applier.dto;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.ApplierRegistryKey;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @Author Slight
 * Dec 01, 2019
 */
public class ApplierConfigDtoTest {

    @Test
    public void isComparable() {
        ApplierConfigDto config1 = new ApplierConfigDto();
        ApplierConfigDto config2 = new ApplierConfigDto();
        String includedDbs = RandomStringUtils.randomAlphabetic(10) + "," + RandomStringUtils.randomAlphabetic(20);
        int applyMode = 1;
        String routeInfo = RandomStringUtils.randomAlphabetic(20);
        Lists.newArrayList(config1, config2).forEach(config->{
            config.target = new DBInfo();
            config.target.ip = "127.0.0.1";
            config.target.port = 4321;
            config.target.uuid = "hello_mysql";
            config.target.password = "123456root";
            config.target.username = "root";
            config.replicator = new InstanceInfo();
            config.replicator.ip = "127.0.0.2";
            config.replicator.port = 1234;
            config.setIncludedDbs(includedDbs);
            config.setApplyMode(applyMode);
            config.setRouteInfo(routeInfo);
            config.setSkipEvent("false");
        });
        assertEquals(config1.toString(), config2.toString());

        System.out.println(JsonUtils.toJson(config1));

        config2.target.username = "hello";
        config2.setIncludedDbs(RandomStringUtils.randomAlphabetic(30));
        assertNotEquals(config1.toString(), config2.toString());
    }

    @Test
    public void testSwitch() {
        ApplierConfigDto config1 = new ApplierConfigDto();
        ApplierConfigDto config2 = new ApplierConfigDto();
        Lists.newArrayList(config1, config2).forEach(config->{
            config.target = new DBInfo();
            config.target.ip = "127.0.0.1";
            config.target.uuid = "hello_mysql";
            config.target.password = "123456root";
            config.target.username = "root";
            config.replicator = new InstanceInfo();
            config.replicator.ip = "127.0.0.2";
            config.replicator.port = 1234;
        });
        assertEquals(config1, config2);

        config2.target.ip = "127.0.0.2";
        assertNotEquals(config1, config2);
    }

    @Test
    public void testJson() {
        ApplierConfigDto config1 = new ApplierConfigDto();
        setApplierConfigDto(config1);

        byte[] bytes = Codec.DEFAULT.encodeAsBytes(config1);
        ApplierConfigDto config2 = Codec.DEFAULT.decode(bytes, ApplierConfigDto.class);
        Assert.assertEquals(config2.getRegistryKey(), ApplierRegistryKey.from("applierMhaName", "testName", "replicatorMhaName"));
        Assert.assertEquals(config1, config2);
    }

    @Test
    public void testExecutedGtid() {
        ApplierConfigDto config1 = new ApplierConfigDto();
        setApplierConfigDto(config1);
        byte[] bytes = Codec.DEFAULT.encodeAsBytes(config1);
        ApplierConfigDto config2 = Codec.DEFAULT.decode(bytes, ApplierConfigDto.class);
        config2.setGtidExecuted("123");
        Assert.assertFalse(config1.equals(config2));  // when first empty, second not empty, restart applier
        Assert.assertTrue(config2.equals(config1));  // when first not empty, second empty, not restart applier, skip
    }

    private void setApplierConfigDto(ApplierConfigDto configDto) {
        Lists.newArrayList(configDto).forEach(config->{
            config.cluster = "testName";
            config.target = new DBInfo();
            config.target.ip = "127.0.0.1";
            config.target.uuid = "hello_mysql";
            config.target.password = "123456root";
            config.target.username = "root";
            config.target.cluster = "testName";
            config.target.mhaName = "applierMhaName";
            config.replicator = new InstanceInfo();
            config.replicator.mhaName = "replicatorMhaName";
            config.replicator.cluster = "testName";
            config.setIncludedDbs(RandomStringUtils.randomAlphabetic(10) + "," + RandomStringUtils.randomAlphabetic(20));
            config.setRouteInfo(RandomStringUtils.randomAlphabetic(20));
            config.setSkipEvent("true");
        });
    }
}
