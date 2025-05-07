package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/7/3 16:48
 */
public class DbMqEditDtoTest {
    @Test
    public void testValid() {
        DbMqEditDto dbMqCreateDto = new DbMqEditDto();
        dbMqCreateDto.setDbReplicationIds(Lists.newArrayList(1L, 2L));
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("originTable");
        originLogicTableConfig.setDstLogicTable("bbz.test.binlog");
        dbMqCreateDto.setOriginLogicTableConfig(originLogicTableConfig);
        dbMqCreateDto.setDalclusterName("test_dalcluster");
        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName("test");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        logicTableConfig.setDstLogicTable("bbz.teset.binlog");
        dbMqCreateDto.setLogicTableConfig(logicTableConfig);
        MqConfigDto mqConfig = new MqConfigDto();
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("orderId");
        mqConfig.setBu("bbz");
        mqConfig.setMqType("qmq");
        mqConfig.setSerialization("json");
        dbMqCreateDto.setMqConfig(mqConfig);
        dbMqCreateDto.validAndTrim();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoDbReplicationIds() {
        DbMqEditDto dbMqCreateDto = new DbMqEditDto();
        dbMqCreateDto.setDbReplicationIds(Lists.newArrayList());
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("originTable");
        dbMqCreateDto.setOriginLogicTableConfig(originLogicTableConfig);
        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName("test");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        dbMqCreateDto.setLogicTableConfig(logicTableConfig);
        dbMqCreateDto.setMqConfig(new MqConfigDto());
        dbMqCreateDto.validAndTrim();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoOriginLogicTableConfig() {
        DbMqEditDto dbMqCreateDto = new DbMqEditDto();
        dbMqCreateDto.setDbReplicationIds(Lists.newArrayList(1L, 2L));
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        dbMqCreateDto.setOriginLogicTableConfig(originLogicTableConfig);
        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName("test");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        dbMqCreateDto.setLogicTableConfig(logicTableConfig);
        dbMqCreateDto.setMqConfig(new MqConfigDto());
        dbMqCreateDto.validAndTrim();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyLogicTableConfig() {
        DbMqEditDto dbMqCreateDto = new DbMqEditDto();
        dbMqCreateDto.setDbReplicationIds(Lists.newArrayList(1L, 2L));
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("originTable");
        dbMqCreateDto.setOriginLogicTableConfig(originLogicTableConfig);

        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName("test");
        dbMqCreateDto.setLogicTableConfig(new LogicTableConfig());
        dbMqCreateDto.setMqConfig(new MqConfigDto());
        dbMqCreateDto.validAndTrim();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoSrcRegionName() {
        DbMqEditDto dbMqCreateDto = new DbMqEditDto();
        dbMqCreateDto.setDbReplicationIds(Lists.newArrayList(1L, 2L));
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("originTable");
        dbMqCreateDto.setOriginLogicTableConfig(originLogicTableConfig);

        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName(null);
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        dbMqCreateDto.setLogicTableConfig(logicTableConfig);
        dbMqCreateDto.setMqConfig(new MqConfigDto());
        dbMqCreateDto.validAndTrim();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoDbNames() {
        DbMqEditDto dbMqCreateDto = new DbMqEditDto();
        dbMqCreateDto.setDbReplicationIds(Lists.newArrayList(1L, 2L));
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("originTable");
        dbMqCreateDto.setOriginLogicTableConfig(originLogicTableConfig);

        dbMqCreateDto.setDbNames(Lists.newArrayList());
        dbMqCreateDto.setSrcRegionName("test");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        dbMqCreateDto.setLogicTableConfig(logicTableConfig);
        dbMqCreateDto.setMqConfig(new MqConfigDto());
        dbMqCreateDto.validAndTrim();
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme