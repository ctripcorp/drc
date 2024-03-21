package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class DbTransactionTableGtidReaderTest {

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }


    @Test
    public void testGetDbGtidSetByUuid() throws Exception {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(resultSet.getLong(1)).thenReturn(79700001L).thenReturn(79731247L);
        Mockito.when(resultSet.getString(2)).thenReturn("5e54ab78-5854-11ee-9136-fa163e6063c9:1-79728308").thenReturn(null).thenReturn(null);

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(statement.executeQuery("select `gno`, `gtidset` from `drcmonitordb`.`tx_db1` where `server_uuid` = \"5e54ab78-5854-11ee-9136-fa163e6063c9\";")).thenReturn(resultSet);

        Connection jdbcConnection = Mockito.mock(Connection.class);
        Mockito.when(jdbcConnection.createStatement()).thenReturn(statement);


        DbTransactionTableGtidReader dbTransactionTableGtidReader = new DbTransactionTableGtidReader("db1");
        GtidSet result = dbTransactionTableGtidReader.getGtidSetByUuid(jdbcConnection, "5e54ab78-5854-11ee-9136-fa163e6063c9");
        Assert.assertEquals(new GtidSet("5e54ab78-5854-11ee-9136-fa163e6063c9:1-79728308:79731247"), result);
    }

    @Test
    public void testGetGtidSetByUuid() throws Exception {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(resultSet.getLong(1)).thenReturn(79700001L).thenReturn(79731247L);
        Mockito.when(resultSet.getString(2)).thenReturn("5e54ab78-5854-11ee-9136-fa163e6063c9:1-79728308").thenReturn(null).thenReturn(null);

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(statement.executeQuery("select `gno`, `gtidset` from `drcmonitordb`.`gtid_executed` where `server_uuid` = \"5e54ab78-5854-11ee-9136-fa163e6063c9\";")).thenReturn(resultSet);

        Connection jdbcConnection = Mockito.mock(Connection.class);
        Mockito.when(jdbcConnection.createStatement()).thenReturn(statement);


        TransactionTableGtidReader transactionTableGtidReader = new TransactionTableGtidReader();
        GtidSet result = transactionTableGtidReader.getGtidSetByUuid(jdbcConnection, "5e54ab78-5854-11ee-9136-fa163e6063c9");
        Assert.assertEquals(new GtidSet("5e54ab78-5854-11ee-9136-fa163e6063c9:1-79728308:79731247"), result);
    }

}