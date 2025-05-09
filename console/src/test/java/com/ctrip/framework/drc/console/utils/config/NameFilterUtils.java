package com.ctrip.framework.drc.console.utils.config;

import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.core.util.NameUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName NameFilterUtils
 * @Author haodongPan
 * @Date 2023/4/19 14:10
 * @Version: $
 */
public class NameFilterUtils {

    private static final Logger logger = LoggerFactory.getLogger(NameFilterUtils.class);

    @Test
    public void getApplyMode() {
        Assert.assertEquals(ApplyMode.mq, NameUtils.getMessengerApplyMode("mha1_dalcluster.mha1._drc_mq"));
        Assert.assertEquals(ApplyMode.db_mq, NameUtils.getMessengerApplyMode("mha1_dalcluster.mha1._drc_mq.xxxdb"));
        Assert.assertEquals(ApplyMode.kafka, NameUtils.getMessengerApplyMode("mha1_dalcluster.mha1._drc_kafka"));
    }

    @Test
    public void getMqTypeMode() {
        Assert.assertEquals(MqType.qmq, NameUtils.getMessengerMqType("mha1_dalcluster.mha1._drc_mq"));
        Assert.assertEquals(MqType.qmq, NameUtils.getMessengerMqType("mha1_dalcluster.mha1._drc_mq.xxxdb"));
        Assert.assertEquals(MqType.kafka, NameUtils.getMessengerMqType("mha1_dalcluster.mha1._drc_kafka"));
    }


    @Test
    public void testGetMessengerRegistryKey() {
        Assert.assertEquals("mha1_dalcluster.mha1._drc_mq",
                NameUtils.getMessengerRegisterKey("mha1_dalcluster.mha1", new Messenger().setApplyMode(ApplyMode.mq.getType())));

        Assert.assertEquals("mha1_dalcluster.mha1._drc_mq.db1",
                NameUtils.getMessengerRegisterKey("mha1_dalcluster.mha1", new Messenger().setApplyMode(ApplyMode.db_mq.getType()).setIncludedDbs("db1")));

        Assert.assertEquals("mha1_dalcluster.mha1._drc_kafka",
                NameUtils.getMessengerRegisterKey("mha1_dalcluster.mha1", new Messenger().setApplyMode(ApplyMode.kafka.getType())));
    }

    @Test
    public void distinctAndUnion(){
        // input dbName & tableNames &(commonPrefix) check and get nameFilter
        String dbName = "";
        String tableNames =""
                ;
        String commonPrefix = "";


        List<String> tables = removeBlankAndSplit(tableNames);
        Collections.sort(tables);
        distinctAndCount(tables);

//        String nameFilter = combine(dbName, tables);
        String nameFilter = combine(dbName,tables,commonPrefix);
        logger.info("nameFilter as follows: \n{}",nameFilter);
    }


    @Test
    public void testGetMessengerDbName() {
        Assert.assertEquals("_drc_mq.dstDB", NameUtils.getMessengerDbName("name.MhaName._drc_mq.dstDB"));
        Assert.assertEquals("_drc_mq", NameUtils.getMessengerDbName("name.MhaName._drc_mq"));
        Assert.assertEquals("_drc_kafka", NameUtils.getMessengerDbName("name.MhaName._drc_kafka"));
        Assert.assertNull(NameUtils.getMessengerDbName("somethingWrong.ehhe"));

        Assert.assertEquals("_drc_mq.testDB", NameUtils.getMessengerDbName(new Messenger().setIncludedDbs("testDB").setApplyMode(ApplyMode.db_mq.getType())));
        Assert.assertEquals("_drc_mq", NameUtils.getMessengerDbName(new Messenger().setApplyMode(ApplyMode.mq.getType())));
        Assert.assertEquals("_drc_kafka", NameUtils.getMessengerDbName(new Messenger().setApplyMode(ApplyMode.kafka.getType())));
    }


    private List<String> removeBlankAndSplit(String tableNames) {
        tableNames = tableNames.replaceAll(" ", "");
        String[] tablesArray = tableNames.split(",");
        return Lists.newArrayList(tablesArray);
    }

    private void distinctAndCount(List<String> tables){
        HashMap<String, Integer> tableCountMap = Maps.newHashMap();
        Iterator<String> iterator = tables.iterator();
        while (iterator.hasNext()) {
            String table = iterator.next();
            Integer count = tableCountMap.get(table);
            if (count == null) {
                tableCountMap.put(table,1);
            } else {
                logger.error("RepeatTable: {}, repeatCount: {}",table,count);
                iterator.remove();
                tableCountMap.put(table,++count);
            }
        }
        logger.info("count after distinct, size: {}",tables.size());
    }


    private String combine(String dbName,List<String> tables) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i == 0) {
                builder.append("(");
            } else {
                builder.append("|");
            }
            builder.append(tables.get(i));
            if (i == tables.size() -1) {
                builder.append(")");
            }
        }
        return  dbName + "\\." + builder;
    }

    private String combine(String dbName,List<String> tables,String commonPrefix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i == 0) {
                builder.append(commonPrefix).append("(");
            } else {
                builder.append("|");
            }
            String table = tables.get(i);
            if (!table.startsWith(commonPrefix)) {
                throw new IllegalArgumentException(table + " notStartWith " + commonPrefix);
            }
            builder.append(table.substring(commonPrefix.length()));
            if (i == tables.size() -1) {
                builder.append(")");
            }
        }
        return  dbName + "\\." + builder;
    }
}
