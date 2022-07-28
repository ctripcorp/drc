package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import static com.ctrip.framework.drc.applier.resource.context.BatchTransactionContextResource.MAX_BATCH_EXECUTE_SIZE;
import static com.ctrip.framework.drc.applier.resource.context.TransactionContextResource.COMMIT;
import static com.ctrip.framework.drc.applier.resource.context.TransactionContextResource.ROLLBACK;

/**
 * @Author limingdong
 * @create 2021/2/3
 */
public abstract class AbstractPartialTransactionContextResource {

    protected TransactionContextResource transactionContextResource;

    protected BatchPreparedStatementExecutor batchPreparedStatementExecutor;

    protected TransactionContextResource parent = new BatchTransactionContextResource();

    @Mock
    protected DataSource dataSource;

    @Mock
    protected Connection connection;

    @Mock
    protected Statement statement;

    @Mock
    protected List<List<Object>> beforeRows;

    protected List<Object> beforeRow = Lists.newArrayList(new String("123"));

    @Mock
    protected Bitmap beforeBitmap;

    @Mock
    protected List<List<Object>> afterRows;

    @Mock
    protected Bitmap afterBitmap;

    @Mock
    protected Columns columns;

    @Mock
    protected List<Bitmap> bitmapsOfIdentifier;

    protected Bitmap bitmapOfIdentifier = Bitmap.from(Lists.newArrayList(Boolean.TRUE));

    @Mock
    protected com.mysql.jdbc.PreparedStatement insertPreparedStatement;

    @Mock
    protected com.mysql.jdbc.PreparedStatement updatePreparedStatement;

    @Mock
    protected com.mysql.jdbc.PreparedStatement deletePreparedStatement;

    @Mock
    protected com.mysql.jdbc.PreparedStatement rollbackPreparedStatement;

    @Mock
    protected com.mysql.jdbc.PreparedStatement commitPreparedStatement;

    @Mock
    protected SQLException sqlException;

    protected static final String GTID = "test_gtid";

    protected static final long SN = 12345654321l;

    public static int ROW_SIZE = new Random().nextInt(100) + 1;

    static {
        if (System.getProperty(SystemConfig.MAX_BATCH_EXECUTE_SIZE) == null) {
            System.setProperty(SystemConfig.MAX_BATCH_EXECUTE_SIZE, String.valueOf(ROW_SIZE * 2 + 1));
        }
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        parent.dataSource = dataSource;
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        if (bigTransaction()) {
            if (parent instanceof BatchTransactionContextResource) {
                ((BatchTransactionContextResource)parent).setBigTransaction(true);
            }
        }
        parent.initialize();

        Mockito.when(beforeRows.get(Mockito.anyInt())).thenReturn(beforeRow);
        Mockito.when(afterRows.get(Mockito.anyInt())).thenReturn(beforeRow);

        Mockito.when(insertPreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("INSERT"))).thenReturn(insertPreparedStatement);

        Mockito.when(updatePreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("UPDATE"))).thenReturn(updatePreparedStatement);

        Mockito.when(deletePreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("DELETE"))).thenReturn(deletePreparedStatement);

        Mockito.when(connection.prepareStatement(ROLLBACK)).thenReturn(rollbackPreparedStatement);
        Mockito.when(connection.prepareStatement(COMMIT)).thenReturn(commitPreparedStatement);

        batchPreparedStatementExecutor = new BatchPreparedStatementExecutor(connection.createStatement());
        transactionContextResource = getBatchPreparedStatementExecutor(parent);
        if (transactionContextResource != parent) {
            transactionContextResource.initialize();
        }

        transactionContextResource.updateGtid(GTID);
        transactionContextResource.updateSequenceNumber(SN);
        TableKey tableKey = new TableKey("test_db_name", "test_table_name");
        transactionContextResource.updateTableKey(tableKey);

    }

    @Test
    public void testMultiTableConflict() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{0, 0});
        Mockito.when(beforeRows.size()).thenReturn(MAX_BATCH_EXECUTE_SIZE / 2 + 1);
        Mockito.when(afterRows.size()).thenReturn(MAX_BATCH_EXECUTE_SIZE / 2 + 1);
        Mockito.when(columns.getBitmapsOfIdentifier()).thenReturn(bitmapsOfIdentifier);

        Mockito.when(columns.getLastBitmapOnUpdate()).thenReturn(bitmapOfIdentifier);
        Mockito.when(bitmapsOfIdentifier.get(Mockito.anyInt())).thenReturn(bitmapOfIdentifier);
        Mockito.when(beforeBitmap.onBitmap(Mockito.any(Bitmap.class))).thenReturn(bitmapOfIdentifier);

        TableKey tableKey = TableKey.from("1", "1");
        transactionContextResource.setTableKey(tableKey);
        transactionContextResource.delete(beforeRows, beforeBitmap, columns);

        tableKey = TableKey.from("2", "2");
        transactionContextResource.setTableKey(tableKey);
        transactionContextResource.delete(beforeRows, beforeBitmap, columns);
        if (transactionContextResource instanceof Batchable) {
            ((Batchable)transactionContextResource).executeBatch();
        }
        // batch and conflict,so * 2
        int coefficient = bigTransaction() ? 2 : 1;
        Mockito.verify(connection, Mockito.times((MAX_BATCH_EXECUTE_SIZE / 2 + 1) * 2 * coefficient)).prepareStatement(Mockito.anyString());
        Mockito.verify(connection, Mockito.times((MAX_BATCH_EXECUTE_SIZE / 2 + 1) * coefficient)).prepareStatement(Mockito.matches("DELETE FROM `1`.`1`"));
        Mockito.verify(connection, Mockito.times((MAX_BATCH_EXECUTE_SIZE / 2 + 1) * coefficient)).prepareStatement(Mockito.matches("DELETE FROM `2`.`2`"));
    }

    protected TransactionContextResource getBatchPreparedStatementExecutor(TransactionContextResource parent) {
        return new PartialTransactionContextResource(parent, true);
    }

    protected boolean bigTransaction() {
        return false;
    }
}
