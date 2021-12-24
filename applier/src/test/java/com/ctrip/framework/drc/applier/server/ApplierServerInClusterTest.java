package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResource;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Dec 02, 2019
 */
public class ApplierServerInClusterTest {

    @Test
    public void testEquals() throws Exception {
        ApplierConfigDto config1 = new ApplierConfigDto();
        ApplierConfigDto config2 = new ApplierConfigDto();
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
        ApplierServerInCluster server = new ApplierServerInCluster(config2);
        assertEquals(server.config, config1);
        assertFalse(server.config == config1);
        assert server.config != config1;

        config1.target.ip = "127.0.0.2";
        assertNotEquals(server.config, config1);

        DataSourceResource dataSourceResource = server.getDataSourceResource();
        Assert.assertNotNull(dataSourceResource);
    }
}