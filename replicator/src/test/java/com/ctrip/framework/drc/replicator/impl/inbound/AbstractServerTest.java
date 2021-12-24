package com.ctrip.framework.drc.replicator.impl.inbound;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.replicator.MockTest;

/**
 * @Author limingdong
 * @create 2021/12/24
 */
public abstract class AbstractServerTest extends MockTest {

    protected static final String IP = "127.0.0.1";

    protected static final int PORT = 3306;

    protected static final String USER = SystemConfig.MYSQL_USER_NAME;

    protected static final String PASSWORD = SystemConfig.MYSQL_PASSWORD;

    protected static final String DESTINATION = SystemConfig.INTEGRITY_TEST;

    protected static final String MHA_NAME = SystemConfig.MHA_NAME_TEST;

    protected static final int REPLICATOR_MASTER_PORT = 8383;

}
