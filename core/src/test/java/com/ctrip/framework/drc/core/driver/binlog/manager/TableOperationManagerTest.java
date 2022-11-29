package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.manager.AbstractSchemaManager.transformPartition;
import static com.ctrip.framework.drc.core.driver.binlog.manager.TableOperationManager.transformAlterPartition;
import static com.ctrip.framework.drc.core.driver.binlog.manager.TableOperationManager.transformCreatePartition;
import static com.ctrip.framework.drc.core.driver.binlog.manager.TableOperationManager.transformTableComment;

/**
 * @Author limingdong
 * @create 2022/11/9
 */
public class TableOperationManagerTest {

    @Test
    public void testTransformAlterPartition() {
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1 ADd   \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1 DROP   \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  dISCARD \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  IMPORT \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  coALESCE \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  REORGANIZE \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  exCHANGE \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  REMOVE \nPARTITIONING p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  UPGRADE \nPARTITIONING p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1 REBuILD   \nPARTITiON p0, p1;"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1  OPTIMIZE     pARTITION \n p0, p1;\n"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1 ANALYZE PARTITION p3;\n".toLowerCase()));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE t1 REPAIR \nPARTITION p0,p1"));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE trb3 \nCHECK PARTITION p1;".toUpperCase()));
        Assert.assertTrue(transformAlterPartition("ALTER TABLE trb4 \ntruncate PARTITION"));

        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1 ADdPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1 DrOPPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  dISCARDPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  IMPORtPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  coALESCEPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  REORGANIZEPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  exCHANGEpARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  REMOVEPARTITIONING p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  UPGRADepARTITIONING p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1 REBuILDPARTITiON p0, p1;"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1  OPTIMIZEpARTITION \n p0, p1;\n"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1 ANALYZEPARTITION p3;\n".toLowerCase()));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE t1 REPAIRPARTITION p0,p1"));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE trb3 \nCHECKPARTITION p1;".toUpperCase()));
        Assert.assertFalse(transformAlterPartition("ALTER TABLE trb4 \ntruncatePARTITION"));
    }

    @Test
    public void testTransformCreatePartition() {
        String queryString = "CREATE TABLE `drc1`.`transfer_log` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
                "  `ureid` varchar(20) NOT NULL DEFAULT '' COMMENT '',\n" +
                "  `us` int(11) DEFAULT NULL COMMENT '',\n" +
                "  `message` varchar(50) DEFAULT NULL COMMENT '',\n" +
                "  `batcNo` varchar(5) DEFAULT NULL COMMENT '',\n" +
                "  `statusDesc` varchar(50) DEFAULT NULL COMMENT '',\n" +
                "  `datachange_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
                "  PRIMARY KEY (`id`,`datachange_lasttime`),\n" +
                "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`),\n" +
                "  KEY `ix_uid` (`uid`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=''\n" +
                "/*!50100 PARTITIon  BY RANGE (to_days(datachange_lasttime))\n" +
                "(PARTITION p20220321 VALUES LESS THAN (738607) ENGINE = InnoDB,\n" +
                " PARTITION p20220328 VALUES LESS THAN (738614) ENGINE = InnoDB,\n" +
                " PARTITION p20220404 VALUES LESS THAN (738621) ENGINE = InnoDB,\n" +
                " PARTITION p20220411 VALUES LESS THAN (738628) ENGINE = InnoDB,\n" +
                " PARTITION p20220418 VALUES LESS THAN (738635) ENGINE = InnoDB,\n" +
                " PARTITION p20220425 VALUES LESS THAN (738642) ENGINE = InnoDB,\n" +
                " PARTITION p20220502 VALUES LESS THAN (738649) ENGINE = InnoDB,\n" +
                " PARTITION p20220509 VALUES LESS THAN (738656) ENGINE = InnoDB,\n" +
                " PARTITION p20220516 VALUES LESS THAN (738663) ENGINE = InnoDB,\n" +
                " PARTITION p20220523 VALUES LESS THAN (738670) ENGINE = InnoDB,\n" +
                " PARTITION p20220530 VALUES LESS THAN (738677) ENGINE = InnoDB,\n" +
                " PARTITION p20220606 VALUES LESS THAN (738684) ENGINE = InnoDB,\n" +
                " PARTITION p20220613 VALUES LESS THAN (738691) ENGINE = InnoDB,\n" +
                " PARTITION p20220620 VALUES LESS THAN (738698) ENGINE = InnoDB,\n" +
                " PARTITION p20220627 VALUES LESS THAN (738705) ENGINE = InnoDB,\n" +
                " PARTITION p20220704 VALUES LESS THAN (738712) ENGINE = InnoDB,\n" +
                " PARTITION p20220711 VALUES LESS THAN (738719) ENGINE = InnoDB,\n" +
                " PARTITION p20220718 VALUES LESS THAN (738726) ENGINE = InnoDB,\n" +
                " PARTITION p20220725 VALUES LESS THAN (738733) ENGINE = InnoDB,\n" +
                " PARTITION p20220801 VALUES LESS THAN (738740) ENGINE = InnoDB,\n" +
                " PARTITION p20220808 VALUES LESS THAN (738747) ENGINE = InnoDB,\n" +
                " PARTITION p20220815 VALUES LESS THAN (738754) ENGINE = InnoDB,\n" +
                " PARTITION p20220822 VALUES LESS THAN (738761) ENGINE = InnoDB,\n" +
                " PARTITION p20220829 VALUES LESS THAN (738768) ENGINE = InnoDB,\n" +
                " PARTITION p20220905 VALUES LESS THAN (738775) ENGINE = InnoDB,\n" +
                " PARTITION p20220912 VALUES LESS THAN (738782) ENGINE = InnoDB,\n" +
                " PARTITION p20220919 VALUES LESS THAN (738789) ENGINE = InnoDB,\n" +
                " PARTITION p20220926 VALUES LESS THAN (738796) ENGINE = InnoDB,\n" +
                " PARTITION p20221003 VALUES LESS THAN (738803) ENGINE = InnoDB,\n" +
                " PARTITION p20221010 VALUES LESS THAN (738810) ENGINE = InnoDB,\n" +
                " PARTITION p20221017 VALUES LESS THAN (738817) ENGINE = InnoDB,\n" +
                " PARTITION p20221031 VALUES LESS THAN (738831) ENGINE = InnoDB,\n" +
                " PARTITION p20221107 VALUES LESS THAN (738838) ENGINE = InnoDB,\n" +
                " PARTITION p20221114 VALUES LESS THAN (738845) ENGINE = InnoDB,\n" +
                " PARTITION p20221121 VALUES LESS THAN (738852) ENGINE = InnoDB,\n" +
                " PARTITION p20221128 VALUES LESS THAN (738859) ENGINE = InnoDB,\n" +
                " PARTITION p20221205 VALUES LESS THAN (738866) ENGINE = InnoDB,\n" +
                " PARTITION p20221212 VALUES LESS THAN (738873) ENGINE = InnoDB,\n" +
                " PARTITION p20221219 VALUES LESS THAN (738880) ENGINE = InnoDB,\n" +
                " PARTITION p20221226 VALUES LESS THAN (738887) ENGINE = InnoDB,\n" +
                " PARTITION p20230102 VALUES LESS THAN (738894) ENGINE = InnoDB,\n" +
                " PARTITION p20230109 VALUES LESS THAN (738901) ENGINE = InnoDB,\n" +
                " PARTITION p20230116 VALUES LESS THAN (738908) ENGINE = InnoDB,\n" +
                " PARTITION p20230123 VALUES LESS THAN (738915) ENGINE = InnoDB,\n" +
                " PARTITION p20230130 VALUES LESS THAN (738922) ENGINE = InnoDB,\n" +
                " PARTITION pMax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */";

        Pair<Boolean, String> res = transformCreatePartition(queryString);
        Assert.assertTrue(res.getKey());
        String finalQueryString = res.getValue();

        String exceptedQueryString = "CREATE TABLE `drc1`.`transfer_log` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
                "  `ureid` varchar(20) NOT NULL DEFAULT '' COMMENT '',\n" +
                "  `us` int(11) DEFAULT NULL COMMENT '',\n" +
                "  `message` varchar(50) DEFAULT NULL COMMENT '',\n" +
                "  `batcNo` varchar(5) DEFAULT NULL COMMENT '',\n" +
                "  `statusDesc` varchar(50) DEFAULT NULL COMMENT '',\n" +
                "  `datachange_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
                "  PRIMARY KEY (`id`,`datachange_lasttime`),\n" +
                "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`),\n" +
                "  KEY `ix_uid` (`uid`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=''\n" +
                "/*!50100  */";
        Assert.assertEquals(exceptedQueryString, finalQueryString);

        queryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    ParTITION   BY RANGE( YEAR(purchased) )\n" +
                "    SUBPARTItiON BY HASH( TO_DAYS(purchased) )\n" +
                "    SUBPARTITIONS 2 (\n" +
                "        PARTITION p0 VALUES LESS THAN (1990),\n" +
                "        PARTITION p1 VALUES LESS THAN (2000),\n" +
                "        ParTITION p2 VALUES LESS THAN MAXVALUE\n" +
                "    );";

        res = transformCreatePartition(queryString);
        Assert.assertTrue(res.getKey());
        finalQueryString = res.getValue();

        exceptedQueryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    ;";

        Assert.assertEquals(exceptedQueryString, finalQueryString);


        queryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    paRTITION    BY RANGE( YEAR(purchased) )\n" +
                "    SUBPARTITION BY HASH( TO_DAYS(purchased) ) (\n" +
                "        PARTITION p0 VALUES LESS THAN (1990) (\n" +
                "            SUBPARTITION s0,\n" +
                "            SUBPARTITION s1\n" +
                "        ),\n" +
                "        PARTITION p1 VALUES LESS THAN (2000) (\n" +
                "            SUBPARTITION s2,\n" +
                "            SUBPARTITION s3\n" +
                "        ),\n" +
                "        PARTITION p2 VALUES LESS THAN MAXVALUE (\n" +
                "            SUBPARTITION s4,\n" +
                "            SUBPARTITION s5\n" +
                "        )\n" +
                "    );";

        res = transformCreatePartition(queryString);
        Assert.assertTrue(res.getKey());
        finalQueryString = res.getValue();

        exceptedQueryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    ;";

        Assert.assertEquals(exceptedQueryString, finalQueryString);


        queryString = "CREATE TABLE employees (\n" +
                "    id INT NOT NULL,\n" +
                "    fname VARCHAR(30),\n" +
                "    lname VARCHAR(30),\n" +
                "    hired DATE NOT NULL DEFAULT '1970-01-01',\n" +
                "    separated DATE NOT NULL DEFAULT '9999-12-31',\n" +
                "    job_code INT,\n" +
                "    store_id INT\n" +
                ")\n" +
                "PARTITION\n BY LIST(store_id) (\n" +
                "    PARTITION pNorth VALUES IN (3,5,6,9,17),\n" +
                "    PARTITION pEast VALUES IN (1,2,10,11,19,20),\n" +
                "    PARTITION pWest VALUES IN (4,12,13,14,18),\n" +
                "    PARTITION pCentral VALUES IN (7,8,15,16)\n" +
                ");";
        res = transformCreatePartition(queryString);
        Assert.assertTrue(res.getKey());
        finalQueryString = res.getValue();

        exceptedQueryString = "CREATE TABLE employees (\n" +
                "    id INT NOT NULL,\n" +
                "    fname VARCHAR(30),\n" +
                "    lname VARCHAR(30),\n" +
                "    hired DATE NOT NULL DEFAULT '1970-01-01',\n" +
                "    separated DATE NOT NULL DEFAULT '9999-12-31',\n" +
                "    job_code INT,\n" +
                "    store_id INT\n" +
                ")\n" +
                ";";

        Assert.assertEquals(exceptedQueryString, finalQueryString);


        queryString = "CREATE TABLE employees (\n" +
                "    id INT NOT NULL,\n" +
                "    fname VARCHAR(30),\n" +
                "    lname VARCHAR(30),\n" +
                "    hired DATE NOT NULL DEFAULT '1970-01-01',\n" +
                "    separated DATE NOT NULL DEFAULT '9999-12-31',\n" +
                "    job_code INT,\n" +
                "    store_id INT\n" +
                ")\n" +
                "PARTITION BY HASH(store_id)\n" +
                "pARTITIONS \n4;";

        res = transformCreatePartition(queryString);
        Assert.assertTrue(res.getKey());
        finalQueryString = res.getValue();

        exceptedQueryString = "CREATE TABLE employees (\n" +
                "    id INT NOT NULL,\n" +
                "    fname VARCHAR(30),\n" +
                "    lname VARCHAR(30),\n" +
                "    hired DATE NOT NULL DEFAULT '1970-01-01',\n" +
                "    separated DATE NOT NULL DEFAULT '9999-12-31',\n" +
                "    job_code INT,\n" +
                "    store_id INT\n" +
                ")\n" +
                "\n" +
                ";";

        Assert.assertEquals(exceptedQueryString, finalQueryString);


        queryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    ParTITION   BY RANGE( YEAR(purchased) )\n" +
                "    SUBPARTItiON BY HASH( TO_DAYS(purchased) )\n" +
                "    SUBPARTITIONS 2";

        res = transformCreatePartition(queryString);
        Assert.assertTrue(res.getKey());
        finalQueryString = res.getValue();

        exceptedQueryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    \n" +
                "    ";

        Assert.assertEquals(exceptedQueryString, finalQueryString);

        // not match
        queryString = "CREATE TABLE ts (id INT, purchased DATE)\n" +
                "    ParTITIONBY RANGE( YEAR(purchased) )\n" +
                "    SUBPARTItiONBY HASH( TO_DAYS(purchased) )\n" +
                "    SUBPARTITIONS2";

        res = transformCreatePartition(queryString);
        Assert.assertFalse(res.getKey());
        finalQueryString = res.getValue();

        exceptedQueryString = queryString;

        Assert.assertEquals(exceptedQueryString, finalQueryString);
    }

    @Test
    public void testSnapshotTransform() {
        Map<String, Map<String, String>> future = Maps.newHashMap();
        Map<String, String> tables1 = Maps.newHashMap();
        future.put("db1", tables1);
        tables1.put("table1", "CREATE TABLE employees (\n" +
                "    id INT NOT NULL,\n" +
                "    fname VARCHAR(30),\n" +
                "    lname VARCHAR(30),\n" +
                "    hired DATE NOT NULL DEFAULT '1970-01-01',\n" +
                "    separated DATE NOT NULL DEFAULT '9999-12-31',\n" +
                "    job_code INT,\n" +
                "    store_id INT\n" +
                ")\n" +
                "/*!50100 PARTITION BY LIST(store_id) (\n" +
                "    PARTITION pNorth VALUES IN (3,5,6,9,17),\n" +
                "    PARTITION pEast VALUES IN (1,2,10,11,19,20),\n" +
                "    PARTITION pWest VALUES IN (4,12,13,14,18),\n" +
                "    PARTITION pCentral VALUES IN (7,8,15,16)\n" +
                ")  */");



        Map<String, Map<String, String>> current = Maps.newHashMap();
        Map<String, String> currentTables1 = Maps.newHashMap();
        current.put("db1", currentTables1);
        currentTables1.put("table1", "CREATE TABLE employees (" +
                "    id INT NOT NULL," +
                "    fname VARCHAR(30)," +
                "    lname VARCHAR(30)," +
                "    hired DATE NOT NULL DEFAULT '1970-01-01'," +
                "    separated DATE NOT NULL DEFAULT '9999-12-31'," +
                "    job_code INT," +
                "    store_id INT" +
                ")");


        future = transformPartition(future);
        current = transformPartition(current);
        Assert.assertEquals(future, current);
    }

    @Test
    public void testTableCommentTransform() {
        // case 1: create with comment
        String queryString = "CREATE TABLE `insert3` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `addcol` varchar(55) DEFAULT 'addcol' COMMENT '',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8 COMMENT =' gtid ' DELAY_KEY_WRITE=1 ;";
        Pair<Boolean, String> res = transformTableComment(queryString, QueryType.CREATE, "123");
        String exceptedQueryString = "CREATE TABLE `insert3` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `addcol` varchar(55) DEFAULT 'addcol' COMMENT '',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8 COMMENT=\'123\' DELAY_KEY_WRITE=1 ;";
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(exceptedQueryString, res.getValue());

        // test idempotent
        queryString = res.getValue();
        res = transformTableComment(queryString, QueryType.CREATE, "123");
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(queryString, res.getValue());


        // case 2 : create without comment
        queryString = "CREATE TABLE `insert3` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `addcol` varchar(55) DEFAULT 'addcol' COMMENT '',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;";
        res = transformTableComment(queryString, QueryType.CREATE, "123");
        exceptedQueryString = "CREATE TABLE `insert3` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `addcol` varchar(55) DEFAULT 'addcol' COMMENT '',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8 COMMENT=\'123\'";
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(exceptedQueryString, res.getValue());
        // test idempotent
        queryString = res.getValue();
        res = transformTableComment(queryString, QueryType.CREATE, "123");
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(queryString, res.getValue());

        // case 3 : alter with comment
        queryString = "alter table `drc2`.`insert1` ADD COLUMN addcol VARCHAR(55) DEFAULT 'addcol' COMMENT '添加普通' after four, COMMEnt =\n 'NDB_COLUMN=MAX_BLOB_PART_SIZE';";
        res = transformTableComment(queryString, QueryType.ALTER, "123");
        exceptedQueryString = "alter table `drc2`.`insert1` ADD COLUMN addcol VARCHAR(55) DEFAULT 'addcol' COMMENT '添加普通' after four, COMMENT='123';";
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(exceptedQueryString, res.getValue());
        // test idempotent
        queryString = res.getValue();
        res = transformTableComment(queryString, QueryType.ALTER, "123");
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(queryString, res.getValue());

        // case 4 : alter without comment
        queryString = "alter table `drc2`.`insert1` ADD COLUMN addcol VARCHAR(55) DEFAULT 'addcol' COMMENT '添加普通' after four; ";
        res = transformTableComment(queryString, QueryType.ALTER, "123");
        exceptedQueryString = "alter table `drc2`.`insert1` ADD COLUMN addcol VARCHAR(55) DEFAULT 'addcol' COMMENT '添加普通' after four, COMMENT='123'";
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(exceptedQueryString, res.getValue());
        // test idempotent
        queryString = res.getValue();
        res = transformTableComment(queryString, QueryType.ALTER, "123");
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(queryString, res.getValue());

        // case 5 : alter comment
        queryString = "ALTER TABLE t1 comMENT = 'New table comment';";
        res = transformTableComment(queryString, QueryType.ALTER, "123");
        exceptedQueryString = "ALTER TABLE t1 COMMENT='123';";
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(exceptedQueryString, res.getValue());
        // test idempotent
        queryString = res.getValue();
        res = transformTableComment(queryString, QueryType.ALTER, "123");
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(queryString, res.getValue());
    }

}