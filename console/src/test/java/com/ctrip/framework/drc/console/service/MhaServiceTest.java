package com.ctrip.framework.drc.console.service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class MhaServiceTest {
    private static final String CONFLICT_COUNT_QUERY= "sum(sum_over_time(fx.drc.applier.%s.conflict.%s_rcount[%sm])) by (db,table,srcMha,destMha)";


    @Test
    public void testGetDcNameForMha() throws UnsupportedEncodingException {
        String query = String.format(CONFLICT_COUNT_QUERY, "trx", "commit", "1");
        String encodedUrl = URLEncoder.encode(query, StandardCharsets.UTF_8.toString());
        String url = "http://uat.osg.ops.qa.nt.ctripcorp.com/api/19049?query=" + encodedUrl + "&step=60&db=APM-FX";
        System.out.println("Encoded URL: " + url);

    }
}