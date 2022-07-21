package com.ctrip.framework.drc.replicator.impl.oubound;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.AbstractServerTest;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManager;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.BindException;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/24 下午3:07.
 */
public class MySQLMasterServerTest extends AbstractServerTest {

    private static final int PORT = 8383 + 2000;

    private MySQLMasterServer mySQLMasterServer;

    private FileManager fileManager;

    private GtidManager gtidManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private MySQLMasterConfig mySQLMasterConfig = new MySQLMasterConfig();

    private Set<UUID> uuids = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

        mySQLMasterConfig.setPort(PORT);
        mySQLMasterServer = new MySQLMasterServer(mySQLMasterConfig);
        fileManager = new DefaultFileManager(schemaManager, DESTINATION);
        gtidManager = new DefaultGtidManager(fileManager, uuidOperator, replicatorConfig);
        fileManager.setGtidManager(gtidManager);
        fileManager.initialize();
        gtidManager.initialize();
        fileManager.start();
        gtidManager.start();
        mySQLMasterServer.initialize();

    }

    @After
    public void tearDown() throws Exception {
        mySQLMasterServer.stop();
        mySQLMasterServer.dispose();
        fileManager.stop();
        gtidManager.stop();
        fileManager.dispose();
        gtidManager.dispose();
        fileManager.destroy();
    }

    @Test(expected = BindException.class)
    public void testStart() throws Exception {
        mySQLMasterServer.addCommandHandler(new ApplierRegisterCommandHandler(gtidManager, fileManager, null, "ut", ApplyMode.set_gtid.getType()));

        mySQLMasterServer.start();
        Assert.assertTrue(isUsed(PORT));

        MySQLMasterServer second = new MySQLMasterServer(mySQLMasterConfig);
        second.initialize();
        second.start();
    }

}
