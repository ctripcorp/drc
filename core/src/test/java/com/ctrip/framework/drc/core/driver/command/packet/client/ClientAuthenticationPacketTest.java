package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/10/17 上午11:17.
 */
public class ClientAuthenticationPacketTest {

    private static final String DATABASE_NAME = "dbname";

    private static final byte[] SCRUMBLE_BUFF = new byte[] {1,2,3,4,5,6,7,8,9,10};

    private static final String AUTHENTICATION_METHOD = "mysql_native_password";

    private ClientAuthenticationPacket clientAuth;

    private byte[] auth = new byte[]{13, -90, 9, 0, 0, 0, 0, 64, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114, 111, 111, 116, 0, 20, -113, -2, -65, -31, -36, 106, 63, 15, -32, 76, -81, -19, -95, -28, -20, -23, -114, 69, 88, -79, 100, 98, 110, 97, 109, 101, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0};

    @Before
    public void setUp() throws Exception {
        clientAuth = new ClientAuthenticationPacket();
        clientAuth.setCharsetNumber((byte) 33);


        clientAuth.setUsername(SystemConfig.MYSQL_USER_NAME);
        clientAuth.setPassword(SystemConfig.MYSQL_PASSWORD);
        clientAuth.setDatabaseName(DATABASE_NAME);
        clientAuth.setScrumbleBuff(SCRUMBLE_BUFF);
        clientAuth.setAuthPluginName(AUTHENTICATION_METHOD.getBytes());

        byte[] clientAuthPkgBody = clientAuth.toBytes();
        clientAuth.setPacketBodyLength(clientAuthPkgBody.length);
        clientAuth.setPacketSequenceNumber((byte) 1);
    }

    @Test
    public void toBytes() throws IOException {
//        StringBuffer stringBuffer = new StringBuffer();
//        for (int i = 0; i < auth.length; ++i) {
//            stringBuffer.append(auth[i]).append(", ");
//        }
        byte[] bytes = clientAuth.toBytes();
        Assert.assertArrayEquals(auth, bytes);
    }
}