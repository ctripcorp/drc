package com.ctrip.framework.drc.console.dto.v3;
import com.google.common.collect.Lists;
import com.ctrip.framework.drc.console.dto.v3.LogicTableConfig;

import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/7/3 16:44
 */
public class DbMqCreateDtoTest {

    @Test
    public void testValid() {
        DbMqCreateDto dbMqCreateDto = new DbMqCreateDto();
        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName("test");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");
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
    public void testEmptyLogicTableConfig() {
        DbMqCreateDto dbMqCreateDto = new DbMqCreateDto();
        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName("test");
        dbMqCreateDto.setLogicTableConfig(new LogicTableConfig());
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
    public void testNoSrcRegionName() {
        DbMqCreateDto dbMqCreateDto = new DbMqCreateDto();
        dbMqCreateDto.setDbNames(Lists.newArrayList("db1"));
        dbMqCreateDto.setSrcRegionName(null);
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");
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
    public void testNoDbNames() {
        DbMqCreateDto dbMqCreateDto = new DbMqCreateDto();
        dbMqCreateDto.setDbNames(Lists.newArrayList());
        dbMqCreateDto.setSrcRegionName("test");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");

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
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme