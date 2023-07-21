package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.sql.SQLException;
import java.util.List;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * Created by dengquanliang
 * 2023/7/18 18:13
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
public class AbstractIntegrationTest {
    public final static Logger logger = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    private static WriteSqlOperatorWrapper writeSqlOperatorWrapper;
    private static List<String> dbs = Lists.newArrayList("cluster_tbl", "mha_tbl", "cluster_mha_map_tbl", "group_mapping_tbl",
            "mha_group_tbl", "db_tbl", "replicator_group_tbl", "applier_group_tbl", "applier_tbl", "rows_filter_mapping_tbl",
            "rows_filter_tbl", "data_media_tbl", "mha_tbl_v2", "mha_replication_tbl", "mha_db_mapping_tbl", "db_replication_tbl",
            "db_replication_filter_mapping_tbl", "columns_filter_tbl_v2", "columns_filter_tbl", "applier_group_tbl_v2", "applier_tbl_v2",
            "messenger_filter_tbl", "messenger_group_tbl", "dataMediaPair_tbl", "rows_filter_tbl_v2", "messenger_tbl");

    @BeforeClass
    public static void setUp() throws Exception {
        AllTests.initTestDb();
        if (writeSqlOperatorWrapper == null) {
            writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(new DefaultEndPoint(MYSQL_IP, META_DB_PORT, MYSQL_USER, null));
            try {
                writeSqlOperatorWrapper.initialize();
                writeSqlOperatorWrapper.start();
            } catch (Exception e) {
                logger.error("sqlOperatorWrapper initialize: ", e);
            }
        }

        truncateDbs();
    }

    @After
    public void doAfter() throws Exception {
        truncateDbs();
    }

    public static void truncateDbs() {
        String TRUNCATE_TBL = "truncate table fxdrcmetadb.%s";
        for (String db : dbs) {
            String sql = String.format(TRUNCATE_TBL, db);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            try {
                writeSqlOperatorWrapper.write(execution);
            } catch (SQLException throwables) {
                logger.error("Failed truncte table : {}", db, throwables);
            }
        }
    }
}
