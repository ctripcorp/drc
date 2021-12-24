package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.applier.resource.context.sql.StatementExecutorResult;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @Author limingdong
 * @create 2020/10/21
 */
public class BatchTransactionContextResource2Test {

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
        MockitoAnnotations.initMocks(this);
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
        doConflict(context);
        assertResult(context, buildArray(true, true, true, true), 4);
        context.dispose();
    }

    @Test
    public void testLimitedSizeConflict() throws Exception {
        TransactionContextResource context = getTransactionContextResource();
        context.CONFLICT_SIZE = 3;
        doConflict(context);
        assertResult(context, buildArray(true, true, true, true), context.CONFLICT_SIZE - 1);  //will not limit overwriteMap and conflictMap
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

    private void assertResult(TransactionContextResource context, List<Boolean> expected, int size){
        assertEquals(expected, context.getConflictMap());
        assertEquals(expected, context.getOverwriteMap());
        assertEquals(size, context.conflictTransactionLog.getRawSqlList().size());
        assertEquals(size, context.conflictTransactionLog.getRawSqlExecutedResultList().size());
        assertEquals(size, context.conflictTransactionLog.getDestCurrentRecordList().size());
        assertEquals(size, context.conflictTransactionLog.getConflictHandleSqlList().size());
        assertEquals(size, context.conflictTransactionLog.getConflictHandleSqlExecutedResultList().size());
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
