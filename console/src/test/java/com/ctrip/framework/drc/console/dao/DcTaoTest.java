package com.ctrip.framework.drc.console.dao;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.utils.DalUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/5 17:07
 */
public class DcTaoTest {
    private static Logger logger = LoggerFactory.getLogger(DcTaoTest.class);

    @Autowired
    private DcTblDao dcTblDao;

    private static DB testDb;

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            if (testDb == null) {
                // fxdrcmetadb for test
                testDb = getDb(12345);
            }
        } catch (Exception e) {
            logger.error("setUp fail", e);
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            testDb.stop();
        } catch (Exception e) {
            logger.error("tearDown fail...");
        }
    }

    @Test
    public void testBatchInsert() throws SQLException {
        int[] results = DalUtils.getInstance().getDcTblDao().batchInsert(getDcTbls());
        for (int i : results) {
            System.out.print(i + " ");
        }
    }

    public static List<DcTbl> getDcTbls() {
        List<DcTbl> dcTbls = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            DcTbl dcTbl = new DcTbl();
            dcTbl.setId(Long.valueOf(i));
            dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
            dcTbl.setDcName("dc" + i);
            dcTbl.setRegionName("region");
            dcTbls.add(dcTbl);
        }

        return dcTbls;
    }

    private static DB getDb(int port) throws ManagedProcessException {
        DBConfigurationBuilder builder = DBConfigurationBuilder.newBuilder();
        builder.setPort(port);
        builder.addArg("--user=root");
        DB db = DB.newEmbeddedDB(builder.build());
        db.start();
        db.source("db/init.sql");
        return db;
    }
}
