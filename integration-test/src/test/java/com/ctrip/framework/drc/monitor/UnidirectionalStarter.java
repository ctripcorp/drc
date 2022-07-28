package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.monitor.module.AbstractTestStarter;
import com.ctrip.framework.drc.core.config.TestConfig;
import org.junit.Before;
import org.junit.Test;

/**
 *  run doTest to start integrity test WITHOUT any interference
 *
 *  start src and dst mysql instance by port 3306 and 3307 if they are not in use, or pick up the first free port starting from 3306
 *  with docker image 'mysql:5.7'
 *
 *  my.cnf is placed in src/my.cnf and dst/my.cnf respectively
 *
 *  docker ps -a | grep mysql | awk '{print $1}' | grep -v CONTAINER | xargs docker rm -f
 *  to stop all containers
 *  docker volume prune
 * Created by mingdongli
 * 2019/10/14 下午11:31.
 */
public class UnidirectionalStarter extends AbstractTestStarter {

    @Override
    @Before
    public void setUp() {
        super.setUp();
//        unidirectionalReplicateModule.setImage("mysql:5.7.22");
    }

    @Test
    public void doTest() throws Exception {
        unidirectionalReplicateModule.startMySQLModule();
        unidirectionalReplicateModule.startRAModule(getSrcConfig(), getDstConfig());
        unidirectionalReplicateModule.startMonitorModule();
        Thread.currentThread().join();
    }

    private TestConfig getSrcConfig() {
        TestConfig customConfig = new TestConfig();

        // applyMode
        customConfig.setApplyMode(ApplyMode.set_gtid);
        return customConfig;
    }

    private TestConfig getDstConfig() {
        TestConfig customConfig = new TestConfig();

        // applyMode
        customConfig.setApplyMode(ApplyMode.transaction_table);

        // rowsFilter
        customConfig.setRowsFilter(ROW_FILTER_PROPERTIES_REGEX);
        return customConfig;
    }

    private static final String ROW_FILTER_PROPERTIES = "{" +
            "  \"rowsFilters\": [" +
            "    {" +
            "      \"mode\": \"trip_uid\"," +
            "      \"tables\": \"(CommonOrderShard[1-9]DB|CommonOrderShard[1][0-2]DB)\\\\.(basicorder|basicorder_ext)\"," +
            "      \"parameters\": {" +
            "        \"columns\": [" +
            "          \"UID\"" +
            "        ]," +
            "        \"illegalArgument\": false," +
            "        \"fetchMode\": 0," +
            "        \"context\": \"SIN\"" +
            "      }" +
            "    }" +
            "  ]" +
            "}";


    private static final String ROW_FILTER_PROPERTIES_REGEX = "{" +
            "  \"rowsFilters\": [" +
            "    {" +
            "      \"mode\": \"java_regex\"," +
            "      \"tables\": \"drc4.row_filter\"," +
            "      \"parameters\": {" +
            "        \"columns\": [" +
            "          \"uid\"" +
            "        ]," +
            "        \"context\": \"trip.*\"" +
            "      }" +
            "    }" +
            "  ]" +
            "}";
}
