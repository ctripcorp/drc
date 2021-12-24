package com.ctrip.framework.drc.mysql;

import com.wix.mysql.EmbeddedMysql;
import ctrip.framework.drc.mysql.EmbeddedDb;
import org.junit.After;
import org.junit.Test;

/**
 * Platform.detect()
 * @Author limingdong
 * @create 2020/3/7
 */
public class EmbeddedDbTest {

    private EmbeddedMysql embeddedMysql;

    @After
    public void tearDown() throws Exception {
        try{
            embeddedMysql.stop();
            System.out.println("ok");
        } catch (Exception e) {
            System.out.println("error");
        }
    }

    @Test
    public void mysqlServer() {
        EmbeddedDb embeddedDb = new EmbeddedDb();
        embeddedMysql = embeddedDb.mysqlServer();
    }

}
