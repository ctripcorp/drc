package com.ctrip.framework.drc.replicator.impl.oubound;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.replicator.impl.inbound.AbstractServerTest;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.BindException;

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

    private MySQLMasterConfig mySQLMasterConfig = new MySQLMasterConfig();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        mySQLMasterConfig.setPort(PORT);
        mySQLMasterServer = new MySQLMasterServer(mySQLMasterConfig);
        fileManager = new DefaultFileManager(schemaManager, DESTINATION);
        gtidManager = new DefaultGtidManager(fileManager);
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
        mySQLMasterServer.addCommandHandler(new ApplierRegisterCommandHandler(gtidManager, fileManager, null));

        mySQLMasterServer.start();
        Assert.assertTrue(isUsed(PORT));

        MySQLMasterServer second = new MySQLMasterServer(mySQLMasterConfig);
        second.initialize();
        second.start();
    }

}