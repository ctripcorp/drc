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
        Mockito.when(mockResultSet.getString(Mockito.eq(1))).thenReturn("newuuid1:1-100,uuid2:1:50");
        Mockito.when(mockResultSet.getString(Mockito.eq(2))).thenReturn("olduuid1:1-100,uuid2:1:50");

        ShowMasterGtidReader showMasterGtidReader = new ShowMasterGtidReader();
        String result = showMasterGtidReader.getExecutedGtids(mockConn);
        Assert.assertEquals(result, "newuuid1:1-100,uuid2:1:50");

        Mockito.when(mockConfig.getOldGtidSqlSwitch()).thenReturn(true);
        String result2 = showMasterGtidReader.getExecutedGtids(mockConn);
        Assert.assertEquals(result2, "olduuid1:1-100,uuid2:1:50");

        String newGtidStr  = "uuid1:1-100,uuid2:1-100";
        String oldGtidStr = "uuid1:1-100,uuid2:1-100";
        Assert.assertTrue(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

        newGtidStr = "uuid1:1-100,uuid2:1-1100";
        oldGtidStr = "uuid1:1-100,uuid2:1-100";
        Assert.assertFalse(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

        newGtidStr = "uuid1:1-100,uuid2:1-1099";
        oldGtidStr = "uuid1:1-100,uuid2:1-100";
        Assert.assertTrue(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

        newGtidStr = "uuid1:1-100,uuid2:1-100,uuid3:1-10";
        oldGtidStr = "uuid1:1-100,uuid2:1-100";
        Assert.assertFalse(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

        newGtidStr = "uuid1:1-100,uuid2:1-100";
        oldGtidStr = "uuid1:1-100,uuid2:1-100,uuid3:1-10";
        Assert.assertFalse(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

        newGtidStr = "";
        oldGtidStr = "";
        Assert.assertFalse(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

        newGtidStr = null;
        oldGtidStr = null;
        Assert.assertFalse(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));


        newGtidStr = "uuid1:1-100,uuid2:1-100,uuid3:";
        oldGtidStr = "uuid1:1-100,uuid2:1-100,uuid3:";
        Assert.assertFalse(showMasterGtidReader.compareGtid(oldGtidStr,newGtidStr));

    }

}