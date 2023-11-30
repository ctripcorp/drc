package com.ctrip.framework.drc.applier.resource.context;

import static org.junit.Assert.assertEquals;

import com.ctrip.framework.drc.applier.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.applier.resource.context.sql.StatementExecutorResult;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * @Author limingdong
 * @create 2020/10/21
 */
public class BatchTransactionContextResourceTest {

    @InjectMocks
    private BatchTransactionContextResource context;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private DataSource dataSource;

    @Mock
    private PooledConnection pooledConnection;

    @Mock
    private com.mysql.jdbc.PreparedStatement preparedStatement;

    @Mock
    private com.mysql.jdbc.PreparedStatement preparedStatementForConflict;

    @Mock
    private com.mysql.jdbc.PreparedStatement insertPreparedStatement;

    @Mock
    private SQLException sqlException;

    private static final String GTID = "test_gtid";

    private static final String SET_NEXT_GTID = "set gtid_next = '%s'";

    private static final String ROLLBACK = "rollback";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeBatch()).thenReturn(new int[0]);

        String setGtid = String.format(SET_NEXT_GTID, GTID);
        Mockito.when(connection.prepareStatement(setGtid)).thenThrow(SQLException.class);
        Mockito.when(connection.prepareStatement(ROLLBACK)).thenReturn(preparedStatement);
    }

    @Test
    public void testComplete() throws Exception {
        context.initialize();
        context.setGtid(GTID);
        TransactionData.ApplyResult applyResult = context.complete();
        context.dispose();
        Assert.assertEquals(TransactionData.ApplyResult.WHATEVER_ROLLBACK, applyResult);
    }


    @Test
    public void testConflict() throws Exception {
        TransactionContextResource context = getTransactionContextResource();
        context.RECORD_SIZE = 4;
        doConflict(context);
        assertResult(context, 4,4,0,4);
        context.dispose();
    }

    @Test
    public void testLimitedSizeConflict() throws Exception {
        TransactionContextResource context = getTransactionContextResource();
        context.RECORD_SIZE = 3;
        doConflict(context);
        assertResult(context, 4,4,0,3);
        PriorityQueue<ConflictRowLog> cflQueue =  context.trxRecorder.getCflRowLogsQueue();
        assertEquals(3L,cflQueue.poll().getRowId());
        assertEquals(2L,cflQueue.poll().getRowId());
        assertEquals(1L,cflQueue.poll().getRowId());
        context.dispose();
    }

    @Test
    public void testRollbackAndCommitRecord() {
        ConflictTransactionLog conflictTransactionLog = JsonUtils.fromJson(
                "{\"srcMha\":\"phd_test2\",\"dstMha\":\"phd_test1\",\"gtid\":\"f1e60816-432d-11ee-ace8-fa163e4c2412:5071902\",\"handleTime\":1696923534058,\"cflLogs\":[{\"db\":\"migrationdb\",\"table\":\"benchmark\",\"rawSql\":\"/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (11,5,'2023-10-10 15:38:52.959')\",\"rawRes\":\"DUPLICATE_ENTRY\",\"dstRecord\":\"|11|5|2023-10-10 15:38:20.423|\",\"handleSql\":\"/*DRC INSERT 1*/ UPDATE `migrationdb`.`benchmark` SET `id`=11,`drc_id_int`=5,`datachange_lasttime`='2023-10-10 15:38:52.959' WHERE `id`=11 AND `datachange_lasttime`<='2023-10-10 15:38:52.959'\",\"handleSqlRes\":\"UPDATE_COUNT_EQUALS_ONE\",\"rowRes\":0,\"rowId\":3},{\"db\":\"migrationdb\",\"table\":\"benchmark\",\"rawSql\":\"/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (10,4,'2023-10-10 15:38:52.959')\",\"rawRes\":\"DUPLICATE_ENTRY\",\"dstRecord\":\"|10|4|2023-10-10 15:38:20.423|\",\"handleSql\":\"/*DRC INSERT 1*/ UPDATE `migrationdb`.`benchmark` SET `id`=10,`drc_id_int`=4,`datachange_lasttime`='2023-10-10 15:38:52.959' WHERE `id`=10 AND `datachange_lasttime`<='2023-10-10 15:38:52.959'\",\"handleSqlRes\":\"UPDATE_COUNT_EQUALS_ONE\",\"rowRes\":0,\"rowId\":2},{\"db\":\"migrationdb\",\"table\":\"benchmark\",\"rawSql\":\"/*DRC INSERT 0*/ INSERT INTO `migrationdb`.`benchmark` (`id`,`drc_id_int`,`datachange_lasttime`) VALUES (9,3,'2023-10-10 15:38:52.959')\",\"rawRes\":\"DUPLICATE_ENTRY\",\"dstRecord\":\"|9|3|2023-10-10 15:38:20.423|\",\"handleSql\":\"/*DRC INSERT 1*/ UPDATE `migrationdb`.`benchmark` SET `id`=9,`drc_id_int`=3,`datachange_lasttime`='2023-10-10 15:38:52.959' WHERE `id`=9 AND `datachange_lasttime`<='2023-10-10 15:38:52.959'\",\"handleSqlRes\":\"UPDATE_COUNT_EQUALS_ONE\",\"rowRes\":0,\"rowId\":1},{\"db\":\"migrationdb\",\"table\":\"benchmark\",\"rawSql\":\"/*DRC UPDATE 0*/ UPDATE `migrationdb`.`benchmark` SET `id`=1,`drc_id_int`=1000,`datachange_lasttime`='2023-10-10 15:31:39.783' WHERE `id`=1 AND `datachange_lasttime`='2023-10-10 15:31:39.783'\",\"rawRes\":\"UPDATE_COUNT_EQUALS_ZERO\",\"dstRecord\":\"|1|100|2023-10-10 15:36:18.0|\",\"handleSql\":\"handle conflict failed\",\"handleSqlRes\":\"handle conflict failed\",\"rowRes\":1,\"rowId\":4}],\"trxRes\":1}",
                ConflictTransactionLog.class);
        List<ConflictRowLog> conflictRowLogs = conflictTransactionLog.getCflLogs();
        PriorityQueue<ConflictRowLog> queue = new PriorityQueue<>();
        queue.add(conflictRowLogs.get(2)); // rowId 1
        queue.add(conflictRowLogs.get(1)); // rowId 2
        queue.add(conflictRowLogs.get(0)); // rowId 3
        queue.add(conflictRowLogs.get(3)); // rowId 4
        assertEquals(3L,queue.poll().getRowId());
        assertEquals(2L,queue.poll().getRowId());
        assertEquals(1L,queue.poll().getRowId());
        assertEquals(4L,queue.poll().getRowId());
    }
    

    @Test
    public void testLimitedSizeConflictWithRollback() throws Exception {
        TransactionContextResource context = getTransactionContextResource();
        context.RECORD_SIZE = 3;
        doConflictWithRollback(context);
        assertResult(context, 4,4,4,3);
        context.dispose();
    }

    /**
     * update monitor set datachange_lasttime=CURRENT_TIMESTAMP(3) where id=1;
     * @throws Exception
     */
    @Test
    public void testMinimalUpdateOfNoParameter() throws Exception {
        TransactionContextResource context = getTransactionContextResource();
        context.dataSource = this.dataSource;
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.contains("DRC UPDATE 0"))).thenReturn(preparedStatement);
        Mockito.when(connection.prepareStatement(Mockito.contains("AFTER REMOVE WHERE ON_UPDATE"))).thenReturn(preparedStatementForConflict);
        Mockito.when(preparedStatement.execute()).thenThrow(sqlException);
        Mockito.when(preparedStatementForConflict.getUpdateCount()).thenReturn(1);
        Mockito.when(sqlException.getMessage()).thenReturn("No value specified for parameter 3");
        Mockito.when(connection.unwrap(PooledConnection.class)).thenReturn(pooledConnection);

        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.update(
                buildArray(
                        buildArray(4)
                ),
                Bitmap.from(true, false, false, false, false, false),
                buildArray(
                        buildArray("2009-11-08 02:22:00.202")
                ),
                Bitmap.from(false, false, false, false, false, true),
                columns()
        );
        Assert.assertEquals(context.getResult().type, StatementExecutorResult.TYPE.UPDATE_COUNT_EQUALS_ONE);
    }

    /**
     * update monitor set datachange_lasttime=CURRENT_TIMESTAMP(3) where id=1;
     * @throws Exception
     */
    @Test
    public void testMinimalDeleteOfNoParameter() throws Exception {
        TransactionContextResource context = getTransactionContextResource();
        context.dataSource = this.dataSource;
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.contains("DRC DELETE 0"))).thenReturn(preparedStatement);
        Mockito.when(connection.prepareStatement(Mockito.contains("DELETE AFTER REMOVE WHERE ON_UPDATE="))).thenReturn(preparedStatementForConflict);
        Mockito.when(preparedStatement.execute()).thenThrow(sqlException);
        Mockito.when(preparedStatementForConflict.getUpdateCount()).thenReturn(1);
        Mockito.when(sqlException.getMessage()).thenReturn("No value specified for parameter");
        Mockito.when(connection.unwrap(PooledConnection.class)).thenReturn(pooledConnection);

        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.delete(
                buildArray(
                        buildArray(4)
                ),
                Bitmap.from(true, false, false, false, false, false),
                columns()
        );
        Assert.assertEquals(context.getResult().type, StatementExecutorResult.TYPE.UPDATE_COUNT_EQUALS_ONE);
    }

    private void assertResult(TransactionContextResource context, long trxRowNum,long conflictRowNum, long rollbackRowNum,long recordNum){
        assertEquals(trxRowNum, context.trxRecorder.getTrxRowNum());
        assertEquals(conflictRowNum, context.trxRecorder.getConflictRowNum());
        assertEquals(rollbackRowNum, context.trxRecorder.getRollbackRowNum());
        assertEquals(recordNum, context.trxRecorder.getCflRowLogsQueue().size());
    }

    private void doConflict(TransactionContextResource context) throws Exception {
        context.dataSource = this.dataSource;
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(preparedStatement.getUpdateCount()).thenReturn(0);
        Mockito.when(insertPreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("UPDATE"))).thenReturn(preparedStatement);
        Mockito.when(connection.prepareStatement(Mockito.contains("INSERT"))).thenReturn(insertPreparedStatement);
        Mockito.when(connection.unwrap(PooledConnection.class)).thenReturn(pooledConnection);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.update(
                buildArray(
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(5, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(6, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(7, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                buildArray(
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(5, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(6, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(7, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                columns()
        );
    }

    private void doConflictWithRollback(TransactionContextResource context) throws Exception {
        context.dataSource = this.dataSource;
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(preparedStatement.getUpdateCount()).thenReturn(0);
        Mockito.when(insertPreparedStatement.getUpdateCount()).thenThrow(new MySQLIntegrityConstraintViolationException("Duplicate entry '4' for key 'PRIMARY'"));
        Mockito.when(connection.prepareStatement(Mockito.contains("UPDATE"))).thenReturn(preparedStatement);
        Mockito.when(connection.prepareStatement(Mockito.contains("INSERT"))).thenReturn(insertPreparedStatement);
        Mockito.when(connection.unwrap(PooledConnection.class)).thenReturn(pooledConnection);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.update(
                buildArray(
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(5, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(6, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(7, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                buildArray(
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(5, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(6, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(7, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                columns()
        );
    }

    private TransactionContextResource getTransactionContextResource() {
        return new TransactionContextResource();
    }

    protected  <T extends Object> ArrayList<T> buildArray(T... items) {
        return Lists.newArrayList(items);
    }

    Columns columns() {
        String json = "{\"columns\":[" +
                "{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"src_ip\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"dest_ip\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"none\",\"columnDefault\":\"none\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"none2\",\"columnDefault\":\"none2\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false}," +
                "{\"type\":18,\"meta\":3,\"nullable\":false,\"name\":\"datachange_lasttime\",\"charset\":null,\"collation\":null,\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}" +
                "]}";
        return Columns.from(Codec.DEFAULT.decode(json.getBytes(), ApplierColumnsRelatedTest.ColumnContainer.class).columns,
                Lists.<List<String>>newArrayList(
                        Lists.<String>newArrayList("id")
                ));
    }
}
