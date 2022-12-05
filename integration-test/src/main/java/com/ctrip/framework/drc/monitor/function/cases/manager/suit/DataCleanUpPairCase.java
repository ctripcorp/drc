package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.monitor.function.cases.DeleteCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.cases.truncate.DefaultTableTruncate;
import com.ctrip.framework.drc.monitor.function.cases.truncate.TableTruncate;
import com.ctrip.framework.drc.monitor.function.execution.delete.SingleDeleteExecution;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.performance.QPSTestCase;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/11/11 下午3:28.
 */
public class DataCleanUpPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String TRUNCATE1 = "truncate table `drc1`.`insert1`;";
    public static final String TRUNCATE2 = "truncate table `drc2`.`insert1`;";
    public static final String TRUNCATE3 = "truncate table `drc3`.`insert1`;";
    public static final String TRUNCATE4 = "truncate table `drc4`.`insert1`;";
    public static final String TRUNCATE5 = "truncate table `drc4`.`insert2`;";
    public static final String TRUNCATE6 = "truncate table `drc4`.`insert3`;";
    public static final String TRUNCATE7 = "truncate table `drc4`.`multi_type_number`;";
    public static final String TRUNCATE8 = "truncate table `drc4`.`multi_type_number`;";
    public static final String TRUNCATE9 = "truncate table `drc4`.`multi_type_number_unsigned`;";
    public static final String TRUNCATE10 = "truncate table `drc4`.`float_type`;";
    public static final String TRUNCATE11 = "truncate table `drc4`.`charset_type`;";
    public static final String TRUNCATE12 = "truncate table `drc4`.`time_type`;";
    public static final String TRUNCATE13 = "truncate table `drc4`.`time_type_boundary`;";
    public static final String TRUNCATE14 = "truncate table `drc4`.`component`;";
    public static final String TRUNCATE15 = "truncate table `drc4`.`benchmark`;";
    public static final String TRUNCATE16 = "truncate table `drc4`.`update1`;";
    public static final String TRUNCATE17 = "truncate table `drc4`.`table_map`;";
    public static final String TRUNCATE18 = "truncate table `drc4`.`grand_transaction`;";
    public static final String TRUNCATE19 = "truncate table `drc4`.`binlog_minimal_row_image`;";
    public static final String TRUNCATE20 = "truncate table `drc4`.`binlog_noblob_row_image`;";
    public static final String TRUNCATE21 = "truncate table `drc4`.`row_filter`;";
    public static final String TRUNCATE22 = "truncate table `drc1`.`combined_primary_key`;";


    @Override
    protected String getFirstStatement() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> getRestStatement() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> getStatements(DmlTypeEnum dmlType) {
        return null;
    }

    @Override
    protected void doWrite(ReadWriteSqlOperator src) {
        List<String> truncates = Lists.newArrayList();
        truncates.add(TRUNCATE1);
        truncates.add(TRUNCATE2);
        truncates.add(TRUNCATE3);
        truncates.add(TRUNCATE4);
        truncates.add(TRUNCATE5);
        truncates.add(TRUNCATE6);
        truncates.add(TRUNCATE7);
        truncates.add(TRUNCATE8);
        truncates.add(TRUNCATE9);
        truncates.add(TRUNCATE10);
        truncates.add(TRUNCATE11);
        truncates.add(TRUNCATE12);
        truncates.add(TRUNCATE13);
        truncates.add(TRUNCATE14);
        truncates.add(TRUNCATE16);
        truncates.add(TRUNCATE17);
        truncates.add(TRUNCATE18);
        truncates.add(TRUNCATE19);
        truncates.add(TRUNCATE20);
        truncates.add(TRUNCATE21);
        truncates.add(TRUNCATE22);
        for (String s : truncates) {
            //truncate 操作可以在checksum后执行，清除所有记录
            logger.info("[Truncate] {} begin", s);
            Execution deleteExecution = new SingleDeleteExecution(s);
            DeleteCase deleteCase = new QPSTestCase(deleteExecution);
            deleteCase.executeDelete(src);
            logger.info("[Truncate] {} end", s);
        }
    }

    public void doWrite(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        List<String> truncates = Lists.newArrayList();
        truncates.add(TRUNCATE1);
        truncates.add(TRUNCATE2);
        truncates.add(TRUNCATE3);
        truncates.add(TRUNCATE4);
        truncates.add(TRUNCATE5);
        truncates.add(TRUNCATE6);
        truncates.add(TRUNCATE7);
        truncates.add(TRUNCATE8);
        truncates.add(TRUNCATE9);
        truncates.add(TRUNCATE10);
        truncates.add(TRUNCATE11);
        truncates.add(TRUNCATE12);
        truncates.add(TRUNCATE13);
        truncates.add(TRUNCATE14);
        truncates.add(TRUNCATE16);
        truncates.add(TRUNCATE17);
        truncates.add(TRUNCATE18);
        truncates.add(TRUNCATE19);
        truncates.add(TRUNCATE20);
        truncates.add(TRUNCATE21);
        truncates.add(TRUNCATE22);
        for (String truncate : truncates) {
            TableTruncate tableTruncate = new DefaultTableTruncate();
            logger.info("[Truncate] {} begin", truncate);
            tableTruncate.truncateTable(src, truncate);
            tableTruncate.truncateTable(dst, truncate);
            logger.info("[Truncate] {} end", truncate);
        }
    }

    @Override
    protected void doTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {

    }
}
