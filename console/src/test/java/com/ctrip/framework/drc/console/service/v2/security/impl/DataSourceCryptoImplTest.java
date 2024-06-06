package com.ctrip.framework.drc.console.service.v2.security.impl;

import static org.junit.Assert.*;

import com.ctrip.framework.drc.console.service.v2.security.DataSourceCrypto;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Before;
import org.junit.Test;

public class DataSourceCryptoImplTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testEncrypt() {
    }

    @Test
    public void testDecrypt() {
    }

    @Test
    public void testParseByte2HexStr() {
        byte[] bytes = DataSourceCryptoImpl.parseHexStr2Byte("8574622AE689BE2164D63F51E39DF242");
        byte[] bytes1 = HexUtils.fromHexString("8574622AE689BE2164D63F51E39DF242");
        System.out.println(bytes);
        System.out.println(bytes1);
        assertArrayEquals(bytes, bytes1);

        String s = DataSourceCryptoImpl.parseByte2HexStr(bytes);
        String s1 = HexUtils.toHexString(bytes);
        System.out.println(s);
        System.out.println(s1);
        assertEquals(s, s1);
        
    }

    @Test
    public void testParseHexStr2Byte() {
    }
}