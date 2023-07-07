package com.ctrip.framework.drc.console.dao;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.RowsFilterTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        int[] results = DalUtils.getInstance().getDcTblDao().batchInsert(new DalHints().enableIdentityInsert(), getDcTbls());
        System.out.println("result: ");
        for (int i : results) {
            System.out.print(i + " ");
        }

        List<DcTbl> dcTbls = DalUtils.getInstance().getDcTblDao().queryAll();
        for (DcTbl dcTbl : dcTbls) {
            System.out.println(dcTbl);
        }
    }

    @Test
    public void testInsert() throws SQLException {
        MhaTblV2 mha = new MhaTblV2();
        mha.setMhaName("mha");
        MhaTblV2Dao mhaTblV2Dao = new MhaTblV2Dao();
        mhaTblV2Dao.insert(mha);
        MhaTblV2 mha1 = mhaTblV2Dao.queryByMhaName("mha");
        System.out.println(mha);
    }

    @Test
    public void testQuery() throws SQLException {
        RowsFilterTblV2Dao rowsFilterTblV2Dao = new RowsFilterTblV2Dao();
        RowsFilterTblV2 tbl = new RowsFilterTblV2();
        tbl.setDeleted(0);
        tbl.setConfigs("CONFIG");
        tbl.setMode(0);


        RowsFilterTblV2 tbl2 = new RowsFilterTblV2();
        tbl2.setDeleted(0);
        tbl2.setConfigs("CONfig");
        tbl2.setMode(0);

        RowsFilterTblV2 tbl1 = new RowsFilterTblV2();
        tbl1.setDeleted(0);
        tbl1.setConfigs("config");
        tbl1.setMode(0);

        rowsFilterTblV2Dao.batchInsert(Lists.newArrayList(tbl, tbl1, tbl2));
        List<RowsFilterTblV2> configs = rowsFilterTblV2Dao.queryByConfigs(0, "config");
        configs.forEach(System.out::println);

        Map<String, RowsFilterTblV2> configMap = configs.stream().collect(Collectors.toMap(RowsFilterTblV2::getConfigs, Function.identity()));
        System.out.println(configMap);

        RowsFilterTblV2 config = configs.stream().filter(e -> e.getConfigs().equals("config")).findFirst().get();
        RowsFilterTblV2 config1 = configs.stream().filter(e -> e.getConfigs().equals("Config")).findFirst().orElse(null);

        System.out.println(config);
        System.out.println(config1);

    }

    public static List<DcTbl> getDcTbls() {
        List<DcTbl> dcTbls = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            DcTbl dcTbl = new DcTbl();
            dcTbl.setId(Long.valueOf(i));
            dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
            dcTbl.setDcName("dc" + i);
            dcTbl.setRegionName("region");
            dcTbls.add(dcTbl);
        }

        DcTbl dcTbl = new DcTbl();
        dcTbl.setId(Long.valueOf(4));
        dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
        dcTbl.setDcName("dc" + 11);
        dcTbl.setRegionName("region");
        dcTbls.add(dcTbl);

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
