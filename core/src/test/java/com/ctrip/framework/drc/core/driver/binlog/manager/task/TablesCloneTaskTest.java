package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class TablesCloneTaskTest {
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCall() throws Exception {
        Map<String, Map<String, String>> map = Maps.newHashMap();
        HashMap<String, String> tableMap = new HashMap<>();
        tableMap.put("dly_db1", "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`dly_db1` (\n" +
                "  `id` bigint NOT NULL,\n" +
                "  `delay_info` varchar(256) NOT NULL,\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3");
        tableMap.put("dly_db2", "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`dly_db2` (\n" +
                "  `id` bigint NOT NULL,\n" +
                "  `delay_info` varchar(256) NOT NULL,\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3");
        map.put("drcmonitordb", tableMap);
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        Endpoint endpoint = mock(Endpoint.class);
        TablesCloneTask task = new TablesCloneTask(map, endpoint, dataSource, "testKey");
        Boolean call = task.call();
        Assert.assertEquals(Boolean.TRUE, call);
        verify(statement, times(2)).execute(anyString());


    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme