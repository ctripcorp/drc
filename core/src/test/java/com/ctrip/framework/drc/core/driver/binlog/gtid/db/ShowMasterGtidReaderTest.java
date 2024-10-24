package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.mysql.jdbc.Connection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by shiruixin
 * 2024/10/24 20:26
 */
public class ShowMasterGtidReaderTest {
    @Test
    public void testSelect() throws Exception {
        DynamicConfig mockConfig = Mockito.mock(DynamicConfig.class);
        Mockito.when(mockConfig.getOldGtidSqlSwitch()).thenReturn(false);
        MockedStatic<DynamicConfig> theMock = Mockito.mockStatic(DynamicConfig.class);

        theMock.when(() -> DynamicConfig.getInstance()).thenReturn(mockConfig);

        Connection mockConn = Mockito.mock(Connection.class);
        Statement mockStatement = Mockito.mock(Statement.class);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.when(mockConn.createStatement()).thenReturn(mockStatement);
        Mockito.when(mockStatement.executeQuery(Mockito.anyString())).thenReturn(mockResultSet);
        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getString(Mockito.eq(1))).thenReturn("newgtid");
        Mockito.when(mockResultSet.getString(Mockito.eq(2))).thenReturn("oldgtid");

        ShowMasterGtidReader showMasterGtidReader = new ShowMasterGtidReader();
        String result = showMasterGtidReader.getExecutedGtids(mockConn);
        Assert.assertEquals(result, "newgtid");

        Mockito.when(mockConfig.getOldGtidSqlSwitch()).thenReturn(true);
        String result2 = showMasterGtidReader.getExecutedGtids(mockConn);
        Assert.assertEquals(result2, "oldgtid");

    }

}