package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * https://dev.mysql.com/doc/refman/8.0/en/set.html
 *
 * Created by dengquanliang
 * 2024/12/30 19:33
 */
public class SetTypePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private static List<Integer> SET_NUMS = Lists.newArrayList(8, 16, 24, 32, 40, 48, 56, 64);
    private static String INSERT_SQL = "insert into %s (id, name) values (%s, '%s');";
    private static String UPDATE_SQL = "update %s set name = '%s' where id = %s;";
    private static String DELETE_SQL = "delete from  %s where id = %s;";
    private static int ROWS_SIZE = 10;

    private int roundCount = 0;

    public SetTypePairCase() {
        initResource(initInsert(), initUpdate(), initDelete(), initSelect());
    }

    /**
     * CREATE TABLE `test_set8` (
     *   `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
     *   `name` set('a1','a2','a3','a4','a5','a6','a7','a8') NOT NULL DEFAULT 'a8' COMMENT '测试set',
     *   `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
     *   `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
     *   PRIMARY KEY (`id`),
     *   KEY `ix_datachange_lasttime` (`datachange_lasttime`)
     * ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb3 COMMENT='test_set'
     */
    @Override
    protected void doWrite(ReadWriteSqlOperator src, DmlTypeEnum dmlType) {
        if (dmlType == DmlTypeEnum.DELETE) { // final dmlType
            if (++roundCount % 5 == 0) {
                roundCount = 0;
            }
        }
        if (roundCount !=0) {
            return; // skip this round,for test observe
        }

        List<String> statements = getStatements(dmlType);
        for (int i = 0; i < statements.size(); i++) {
            WriteExecution writeExecution = new MultiStatementWriteExecution(Lists.newArrayList(statements.get(i)));
            InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
            boolean affected = insertCase.executeInsert(src);
            if (!affected) {
                alarm.alarm(String.format("%s execute %s error %s", getClass().getSimpleName(), dmlType.getDescription(), writeExecution.getStatements()));
            }
        }
        parkThreeSecond();
    }


    private List<String> initInsert() {
        List<String> list = new ArrayList<>();
        for (int num : SET_NUMS) {
            String table = "testsetdb.test_set" + num;
            list.addAll(getRandomInsertList(table, num));
        }
        return list;
    }

    private List<String> initUpdate() {
        List<String> list = new ArrayList<>();
        for (int num : SET_NUMS) {
            String table = "testsetdb.test_set" + num;
            list.addAll(getRandomUpdateList(table, num));
        }
        return list;
    }

    private List<String> initDelete() {
        List<String> list = new ArrayList<>();
        for (int num : SET_NUMS) {
            String table = "testsetdb.test_set" + num;
            for (int i = 1; i <= ROWS_SIZE; i++) {
                list.add(String.format(DELETE_SQL, table, i));
            }
        }
        return list;
    }

    private String[] initSelect() {
        String[] list = new String[SET_NUMS.size()];
        int i = 0;
        for (int num : SET_NUMS) {
            String table = "testsetdb.test_set" + num;
            list[i++] = String.format("select * from %s;", table);
        }
        return list;
    }


    private List<String> getRandomInsertList(String table, int bound) {
        Random random = new Random();
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= ROWS_SIZE; i++) {
            int num = random.nextInt(bound) + 1;
            String value = getRandomValue(bound, num);
            list.add(String.format(INSERT_SQL, table, i, value));
        }
        return list;
    }

    private List<String> getRandomUpdateList(String table, int bound) {
        Random random = new Random();
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= ROWS_SIZE; i++) {
            int num = random.nextInt(bound) + 1;
            String value = getRandomValue(bound, num);
            list.add(String.format(UPDATE_SQL, table, value, i));
        }
        return list;
    }

    private String getRandomValue(int bound, int num) {
        Random random = new Random();
        StringBuilder value = new StringBuilder();
        for (int j = 0; j < num; j++) {
            int suffix = random.nextInt(bound) + 1;
            value.append("a" + suffix);
            if (j != num - 1) {
                value.append(",");
            }
        }
        return value.toString();
    }

}
