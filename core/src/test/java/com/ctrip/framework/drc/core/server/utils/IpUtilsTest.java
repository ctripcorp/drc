package com.ctrip.framework.drc.core.server.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by mingdongli
 * 2019/10/17 上午9:53.
 */
public class IpUtilsTest {

    public static final String MOCK_IP = "1.2.3.4";

    @Before
    public void setUp() {
        System.setProperty("host.ip", MOCK_IP);
    }

    @Test
    public void getSystemProperty() {
        String IP = IpUtils.getFistNonLocalIpv4ServerAddress();
        assertEquals(MOCK_IP, IP);
    }

    @Test
    public void getFistNonLocalIpv4ServerAddress() {
        String IP = IpUtils.getFistNonLocalIpv4ServerAddress();
        assertNotNull(IP);
    }
}