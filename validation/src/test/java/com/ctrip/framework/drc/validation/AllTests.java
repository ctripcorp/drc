package com.ctrip.framework.drc.validation;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import com.ctrip.framework.drc.validation.container.ValidationServerContainerTest;
import com.ctrip.framework.drc.validation.container.controller.HealtchControllerTest;
import com.ctrip.framework.drc.validation.container.controller.ValidationServerControllerTest;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContextResourceTest;
import com.ctrip.framework.drc.validation.server.LocalValidationServerTest;
import com.ctrip.framework.drc.validation.server.ValidationServerInClusterTest;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ValidationServerContainerTest.class,
        LocalValidationServerTest.class,
        ValidationServerInClusterTest.class,

        ValidationTransactionContextResourceTest.class,

        // controller
        ValidationServerControllerTest.class,
        HealtchControllerTest.class
})

public class AllTests {

    private static Logger logger = LoggerFactory.getLogger(AllTests.class);

    public static final String DC = "ntgxh";
    public static final String DC2 = "shaoy";
    public static final String CLUSTER = "testdb_dalcluster";
    public static final String MHA = "testmha";
    public static final String DATABASE = "db1";
    public static final String TABLE = "tbl1";
    public static final String GTID_EXECUTED = "uuid0:1-10,uuid1:1-100,uuid2:1-1000,uuid3:1-10000";
    public static final String GTID_EXECUTED2 = "uuid0:1-10,uuid1:1-100,uuid2:1-1000";
    public static final String GTID = "uuid0:1";
    public static final String UUID_PREFIX = "uuid";
    public static final String MACHINE_IP_PREFIX = "10.0.0.";
    public static final int MACHINE_PORT_BASE = 3306;
    public static final String REPLICATOR_IP = "100.0.0.1";
    public static final String REPLICATOR_IP2 = "100.0.0.2";
    public static final int REPLICATOR_APPLIER_PORT = 8383;
    public static final Map<String, String> UID_MAP = new HashMap<>() {{
        put("`db1`.`tbl1`", "uid1");
        put("`db2`.`tbl2`", "uid2");
    }};
    public static final Map<String, String> UID_MAP_INCORRECT = new HashMap<>() {{
        put("`db1`.`tbl1`", "uid");
        put("`db2`.`tbl2`", "uid2");
    }};
    public static final Map<String, Integer> UCS_STRATEGY_MAP = new HashMap<>() {{
        put("db1", 1);
        put("db2", 2);
    }};
    public static final Map<String, Integer> UCS_STRATEGY_MAP_INCORRECT = new HashMap<>() {{
        put("db", 1);
        put("db2", 2);
    }};
    public static final String UID_VALUE = "M0001";

    @BeforeClass
    public static void setUp() {
//        System.setProperty("ucs.dev.mode", Boolean.TRUE.toString());
    }

    @AfterClass
    public static void tearDown() {

    }

    public static ValidationConfigDto getValidationConfigDto() {
        ValidationConfigDto dto = new ValidationConfigDto();
        dto.setIdc(DC);
        dto.setCluster(CLUSTER);
        dto.setMhaName(MHA);
        dto.setGtidExecuted(GTID_EXECUTED);
        dto.setMachines(getMachines());
        dto.setReplicator(getReplicator());
        dto.setUidMap(UID_MAP);
        dto.setUcsStrategyIdMap(UCS_STRATEGY_MAP);
        return dto;
    }

    public static List<DBInfo> getMachines() {
        logger.info("getting default machines");
        List<DBInfo> dbInfos = Lists.newArrayList();
        for(int i = 0; i < 2; i++) {
            DBInfo dbInfo = new DBInfo();
            dbInfo.setIdc(DC);
            dbInfo.setUuid(String.format("%s%d", UUID_PREFIX, i));
            dbInfo.setMhaName(MHA);
            dbInfo.setIp(String.format("%s%d", MACHINE_IP_PREFIX, i));
            dbInfo.setPort(MACHINE_PORT_BASE + i);
            dbInfo.setCluster(CLUSTER);
            dbInfos.add(dbInfo);
        }
        return dbInfos;
    }

    public static List<DBInfo> getMachines2() {
        logger.info("getting machines 2");
        List<DBInfo> dbInfos = Lists.newArrayList();
        for(int i = 0; i < 2; i++) {
            DBInfo dbInfo = new DBInfo();
            dbInfo.setIdc(DC2);
            dbInfo.setUuid(String.format("%s-%d", UUID_PREFIX, i));
            dbInfo.setMhaName(MHA);
            dbInfo.setIp(String.format("%s%d", MACHINE_IP_PREFIX, i));
            dbInfo.setPort(MACHINE_PORT_BASE + i);
            dbInfo.setCluster(CLUSTER);
            dbInfos.add(dbInfo);
        }
        return dbInfos;
    }

    public static List<DBInfo> getMachines3() {
        logger.info("getting machines 3");
        List<DBInfo> dbInfos = Lists.newArrayList();
        for(int i = 0; i < 2; i++) {
            DBInfo dbInfo = new DBInfo();
            dbInfo.setIdc(DC2);
            dbInfo.setUuid(String.format("%s%d", UUID_PREFIX, i));
            dbInfo.setMhaName(MHA);
            dbInfo.setIp(String.format("%s%d", MACHINE_IP_PREFIX, i));
            dbInfo.setPort(MACHINE_PORT_BASE + i);
            dbInfo.setCluster(CLUSTER);
            dbInfos.add(dbInfo);
        }
        return dbInfos;
    }

    public static InstanceInfo getReplicator() {
        InstanceInfo info = new InstanceInfo();
        info.setIp(REPLICATOR_IP);
        info.setPort(REPLICATOR_APPLIER_PORT);
        info.setMhaName(MHA);
        info.setCluster(CLUSTER);
        info.setIdc(DC);
        return info;
    }

    public static InstanceInfo getReplicator2() {
        InstanceInfo info = new InstanceInfo();
        info.setIp(REPLICATOR_IP2);
        info.setPort(REPLICATOR_APPLIER_PORT);
        info.setMhaName(MHA);
        info.setCluster(CLUSTER);
        info.setIdc(DC);
        return info;
    }
}
