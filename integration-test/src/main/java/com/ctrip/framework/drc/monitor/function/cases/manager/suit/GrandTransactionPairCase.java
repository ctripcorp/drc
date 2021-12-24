package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.select.SingleTableSelectCase;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.ctrip.framework.drc.monitor.utils.enums.TestTypeEnum;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/27
 */
public class GrandTransactionPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private static final String INSERT = "insert into %s (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`, `drc_id_int`, `addcol`, `addcol1`, `addcol2`, `drc_char_test_2`, `drc_tinyint_test_2`, `drc_bigint_test`, `drc_integer_test`, `drc_mediumint_test`, `drc_time6_test`, `drc_datetime3_test`, `drc_year_test`, `hourly_rate_3`, `drc_numeric10_4_test`, `drc_float_test`, `drc_double_test`, `drc_double10_4_test`, `drc_real_test`, `drc_real10_4_test`) values\n" +
            "('china1', 'かな1', 'присоска1', 'abcdefg', NOW(), 123, 'addcol1', 'addcol2', 'addcol3', 'charvalue', 11, 22, 33, 44, '02:12:22', '2020-04-03 11:11:03', '2008', 1.04, 10.0, 13, 1234, 13.1212, 345, 12.3);";

    private static final String UPDATE = "update `drc4`.`grand_transaction` set `chareq256`='egGrandT' where `id` in (%s);";

    private static final String DELETE = "delete from `drc4`.`grand_transaction` where `id` in (%s);";

    private static final String SELECT = "select * from `drc4`.`grand_transaction`;";

    private List<String> statements = Lists.newArrayList();

    private Map<DmlTypeEnum, List<String>> ret = new HashMap<>();

    private static int ROWS;

    static {
        ROWS = ConfigService.getInstance().getGrandTransactionRows();
    }

    public GrandTransactionPairCase(TestTypeEnum testTypeEnum) {
        String insertSql = String.format(INSERT, "`drc4`.`grand_transaction`");
        for (int i = 0; i < ROWS; ++i) {
            statements.add(insertSql);
        }
        if(testTypeEnum == TestTypeEnum.FUNCTION) {
            initResource(Lists.newArrayList("override"), Lists.newArrayList("override"), Lists.newArrayList("override"), SELECT);
        }
    }

    public GrandTransactionPairCase() {
        String insertSql = String.format(INSERT, "`bbzbbzdrcbenchmarktmpdb`.`benchmark2`");
        for (int i = 0; i < ROWS; ++i) {
            statements.add(insertSql);
        }
    }

    protected List<String> getStatements() {
        return statements;
    }

    @Override
    protected void doWrite(ReadWriteSqlOperator src) {
        try {
            logger.info("[Batch] GrandTransactionPairCase doWrite start, insert statements rows is {}", ROWS);
            WriteExecution writeExecution = new MultiStatementWriteExecution(statements);
            InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
            boolean affected = insertCase.executeInsert(src);
            if (!affected) {
                alarm.alarm(String.format("%s execute write error %s", getClass().getSimpleName(), writeExecution.getStatements()));
            }
            logger.info("GrandTransactionPairCase doWrite stop");
        } catch (Exception e) {
            logger.error("doWrite error", e);
        }
    }

    @Override
    protected void doWrite(ReadWriteSqlOperator src, DmlTypeEnum dmlType) {
        if(DmlTypeEnum.INSERT == dmlType) {
            doWrite(src);
            parkThreeSecond();
            for (int i = 0; i < 10; ++i) {  //write 2500
                executeStatements(Lists.newArrayList(String.format(INSERT, "`drc4`.`grand_transaction`")), src);
            }
            parkOneSecond();
        } else if(DmlTypeEnum.UPDATE == dmlType){
            ret = getFunctionUpdateAndDeleteStatement(src);
            executeStatements(ret.get(DmlTypeEnum.UPDATE), src);
            parkThreeSecond();
        } else if(DmlTypeEnum.DELETE == dmlType) {
            executeStatements(ret.get(DmlTypeEnum.DELETE), src);
            parkThreeSecond();
        } else {
        }
    }

    private void executeStatements(List<String> statements, ReadWriteSqlOperator src) {
        WriteExecution writeExecution = new MultiStatementWriteExecution(statements);
        InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
        insertCase.executeInsert(src);
    }

    private Map<DmlTypeEnum, List<String>> getFunctionUpdateAndDeleteStatement(ReadWriteSqlOperator src) {

        Map<DmlTypeEnum, List<String>> ret = new HashMap<>();

        try {
            Set<Integer> ids = selectPrimaryKey(src);
            if (ids.isEmpty()) {
                logger.info("[Empty] IDS and return");
                return null;
            }
            String idString = StringUtils.join(ids, ",");
            String updateSql = String.format(UPDATE, idString);
            String deleteSql = String.format(DELETE, idString);
            ret.put(DmlTypeEnum.UPDATE, Lists.newArrayList(updateSql));
            ret.put(DmlTypeEnum.DELETE, Lists.newArrayList(deleteSql));
            logger.info("GrandTransactionPairCase update sql is [{}]", updateSql);
            logger.info("GrandTransactionPairCase delete sql is [{}]", deleteSql);
            return ret;
        } catch (Exception e) {
            logger.error("GrandTransactionPairCase get update sql error", e);
        }
        return null;
    }


    private Set<Integer> selectPrimaryKey(ReadWriteSqlOperator src) throws SQLException {
        Set<Integer> ids = Sets.newHashSet();

        String select = "select `id` from `drc4`.`grand_transaction` order by id limit 2500;";
        Execution selectExecution = new SingleSelectExecution(select);
        SelectCase selectCase = new SingleTableSelectCase(selectExecution);
        ReadResource expected = selectCase.executeSelect(src);
        ResultSet resultSet = expected.getResultSet();
        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            ids.add(id);
        }

        logger.info("[ID] is {}", ids);
        return ids;
    }

    @Override
    protected String getFirstStatement() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> getRestStatement() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {

    }
}
