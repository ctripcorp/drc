package com.ctrip.framework.drc.console.service.v2.integration;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.platform.dal.dao.DalHints;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.sql.SQLException;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.*;

/**
 * Created by dengquanliang
 * 2023/6/1 11:27
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
public class MetaGeneratorV2IntegrationTest {

    private static Logger logger = LoggerFactory.getLogger(MetaGeneratorV2IntegrationTest.class);

    @Autowired
    private MetaGeneratorV2 metaGenerator;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private RouteTblDao routeTblDao;
    @Autowired
    private ProxyTblDao proxyTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ClusterManagerTblDao clusterManagerTblDao;
    @Autowired
    private ZookeeperTblDao zookeeperTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private MessengerServiceV2 messengerService;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;

    private static DB testDb;

//    @BeforeClass
//    public static void setUp() throws Exception {
//        try {
//            if (testDb == null) {
//                // fxdrcmetadb for test
//                testDb = getDb(12345);
//            }
//        } catch (Exception e) {
//            logger.error("setUp fail", e);
//        }
//    }
//
//    @AfterClass
//    public static void tearDown() {
//        try {
//            testDb.stop();
//        } catch (Exception e) {
//            logger.error("tearDown fail...");
//        }
//    }

    @Test
    public void testGetDrc() throws Exception {
        initDao();
        Drc drc = metaGenerator.getDrc();
        logger.info("drc: \n{}", drc.toString());

    }

   private void initDao() throws SQLException {
       buTblDao.batchInsert(new DalHints().enableIdentityInsert(), getButbls());
       routeTblDao.batchInsert(new DalHints().enableIdentityInsert(), getRouteTbls());
       proxyTblDao.batchInsert(new DalHints().enableIdentityInsert(), getProxyTbls());
       dcTblDao.batchInsert(new DalHints().enableIdentityInsert(), getDcTbls());
       mhaTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTbls());
       resourceTblDao.batchInsert(new DalHints().enableIdentityInsert(), getResourceTbls());
       machineTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMachineTbls());
       replicatorGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), getReplicatorGroupTbls());
       applierGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), getApplierGroupTbls());
       clusterManagerTblDao.batchInsert(new DalHints().enableIdentityInsert(), getClusterManagerTbls());
       zookeeperTblDao.batchInsert(new DalHints().enableIdentityInsert(), getZookeeperTbls());
       replicatorTblDao.batchInsert(new DalHints().enableIdentityInsert(), getReplicatorTbls());
       applierTblDao.batchInsert(new DalHints().enableIdentityInsert(), getApplierTbls());
       mhaReplicationTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaReplicationTbls());
       mhaDbMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaDbMappingTbls());
       dbReplicationTblDao.batchInsert(new DalHints().enableIdentityInsert(), getDbReplicationTbls());
       dbTblDao.batchInsert(new DalHints().enableIdentityInsert(), getDbTbls());

       messengerGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMessengerGroupTbls());
       messengerTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMessengerTbls());
       dbReplicationFilterMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), getFilterMappingTbls());
       rowsFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), getRowsFilterTbls());
       columnFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), getColumnsFilterTbls());
       messengerFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMessengerFilterTbls());
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
