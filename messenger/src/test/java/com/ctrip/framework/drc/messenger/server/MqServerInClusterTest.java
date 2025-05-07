package com.ctrip.framework.drc.messenger.server;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by shiruixin
 * 2024/11/8 16:55
 */
public class MqServerInClusterTest {
    @Test
    public void testEquals() throws Exception {
        MessengerConfigDto config1 = new MessengerConfigDto();
        MessengerConfigDto config2 = new MessengerConfigDto();
        Lists.newArrayList(config1, config2).forEach(
                config -> {
                    config.target = new DBInfo();
                    config.target.ip = "127.0.0.1";
                    config.target.uuid = "hello_mysql";
                    config.target.password = "123456root";
                    config.target.username = "root";
                    config.replicator = new InstanceInfo();
                    config.replicator.ip = "127.0.0.1";
                    config.replicator.port = 8383;
                    //not a complete config
                    //which is adequate to test equal()
                }
        );
        MqServerInCluster server = new MqServerInCluster(config2);
        assertEquals(server.config, config1);
        assertFalse(server.config == config1);
        assert server.config != config1;

        config1.target.ip = "127.0.0.2";
        assertNotEquals(server.config, config1);

//        DataSourceResource dataSourceResource = server.getDataSourceResource();
//        Assert.assertNotNull(dataSourceResource);
    }
}