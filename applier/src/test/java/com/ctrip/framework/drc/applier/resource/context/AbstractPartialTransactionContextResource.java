package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

/**
 * @Author limingdong
 * @create 2021/2/3
 */
public abstract class AbstractPartialTransactionContextResource {

    protected PartialTransactionContextResource partialTransactionContextResource;

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
    protected SQLException sqlException;

    protected static final String GTID = "test_gtid";

    protected static final long SN = 12345654321l;

    protected static int ROW_SIZE = new Random().nextInt(100) + 1;

    static {
        System.setProperty(SystemConfig.MAX_BATCH_EXECUTE_SIZE, String.valueOf(ROW_SIZE * 2 + 1));
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        parent.dataSource = dataSource;
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        parent.initialize();

        Mockito.when(beforeRows.get(Mockito.anyInt())).thenReturn(beforeRow);
        Mockito.when(afterRows.get(Mockito.anyInt())).thenReturn(beforeRow);

        Mockito.when(insertPreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("INSERT"))).thenReturn(insertPreparedStatement);

        Mockito.when(updatePreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("UPDATE"))).thenReturn(updatePreparedStatement);

        Mockito.when(deletePreparedStatement.getUpdateCount()).thenReturn(1);
        Mockito.when(connection.prepareStatement(Mockito.contains("DELETE"))).thenReturn(deletePreparedStatement);

        batchPreparedStatementExecutor = new BatchPreparedStatementExecutor(connection.createStatement());
        partialTransactionContextResource = getBatchPreparedStatementExecutor(parent);

        partialTransactionContextResource.initialize();

        partialTransactionContextResource.updateGtid(GTID);
        partialTransactionContextResource.updateSequenceNumber(SN);
        TableKey tableKey = new TableKey("test_db_name", "test_table_name");
        partialTransactionContextResource.updateTableKey(tableKey);

    }

    protected PartialTransactionContextResource getBatchPreparedStatementExecutor(TransactionContextResource parent) {
        return new PartialTransactionContextResource(parent, true);
    }
}
