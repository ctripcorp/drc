package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.replicator.AllTests.*;
import static com.ctrip.framework.drc.replicator.container.ReplicatorServerContainerTest.*;

/**
 * @Author limingdong
 * @create 2020/10/12
 */
public class SchemaManagerFactoryTest {

    private ReplicatorConfigDto configDto = new ReplicatorConfigDto();

    private ReplicatorConfig replicatorConfig;

    private String registerKey;

    @Before
    public void setUp() throws Exception {
        configDto.setBu(BU);
        configDto.setClusterAppId(APPID);
        configDto.setMhaName(MHA_NAME);
        configDto.setSrcDcName(System.getProperty(DefaultFoundationService.DATA_CENTER_KEY, GlobalConfig.DC));

        configDto.setStatus(InstanceStatus.ACTIVE.getStatus());
        configDto.setClusterName(CLUSTER_NAME);
        configDto.setGtidSet("");
        configDto.setApplierPort(APPLIER_PORT);

        List<String> uuids = Lists.newArrayList();
        Db db = new Db();
        db.setMaster(true);
        db.setPort(SRC_PORT);
        db.setIp(SRC_IP);
        db.setUuid(UUID);

        uuids.add(UUID);
        configDto.setReadUser(MYSQL_USER);
        configDto.setReadPassward(MYSQL_PASSWORD);
        configDto.setUuids(uuids);
        configDto.setPreviousMaster("");
        configDto.setMaster(db);

        replicatorConfig = configDto.toReplicatorConfig();
        registerKey = replicatorConfig.getRegistryKey();
    }

    @Test
    public void testAllMethods() {
        MySQLSchemaManager mySQLSchemaManager = SchemaManagerFactory.getOrCreateMySQLSchemaManager(replicatorConfig);
        Assert.assertNotNull(mySQLSchemaManager);
        MySQLSchemaManager mySQLSchemaManager2 = SchemaManagerFactory.getOrCreateMySQLSchemaManager(replicatorConfig);
        Assert.assertEquals(mySQLSchemaManager, mySQLSchemaManager2);
        MySQLSchemaManager mySQLSchemaManager3 = SchemaManagerFactory.remove(registerKey);
        Assert.assertEquals(mySQLSchemaManager, mySQLSchemaManager3);

        mySQLSchemaManager = SchemaManagerFactory.getOrCreateMySQLSchemaManager(replicatorConfig);
        SchemaManagerFactory.clear();
        MySQLSchemaManager mySQLSchemaManager4 = SchemaManagerFactory.getOrCreateMySQLSchemaManager(replicatorConfig);
        Assert.assertNotEquals(mySQLSchemaManager, mySQLSchemaManager4);
    }

    @Test
    public void testToLowerCase(){
        Assert.assertNull(MySQLSchemaManager.toLowerCaseIfNotNull(null));
        Assert.assertEquals("test", MySQLSchemaManager.toLowerCaseIfNotNull("test"));
        Assert.assertEquals("test", MySQLSchemaManager.toLowerCaseIfNotNull("Test"));
    }
}