package com.ctrip.framework.drc.core.server.config.cm.dto;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class SchemasHistoryDeltaDtoTest {

    SchemasHistoryDeltaDto deltaDto;

    @Before
    public void setUp() throws Exception {
        deltaDto = new SchemasHistoryDeltaDto();
        deltaDto.setGtidExecuted("a0780e56-d445-11e9-97b4-58a328e0e9f2:1-101");
        deltaDto.setTable("prod.hello");
        deltaDto.setColumns(Lists.newArrayList(
                new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", "PRI", null, "NULL"),
                new TableMapLogEvent.Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, "NULL"),
                new TableMapLogEvent.Column("gender", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, "on update", "NULL")
        ));
    }

    @After
    public void tearDown() throws Exception {
        deltaDto = null;
    }

    @Test
    public void doEncodeDecode() {
        String data = "{\"gtidExecuted\":\"a0780e56-d445-11e9-97b4-58a328e0e9f2:1-101\",\"table\":\"prod.hello\",\"columns\":[{\"type\":3,\"meta\":0,\"nullable\":true,\"name\":\"id\",\"charset\":null,\"collation\":null,\"columnDefault\":\"NULL\",\"unsigned\":false,\"binary\":false,\"pk\":true,\"uk\":false,\"onUpdate\":false},{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"user\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"columnDefault\":\"NULL\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":false},{\"type\":15,\"meta\":60,\"nullable\":true,\"name\":\"gender\",\"charset\":\"utf8\",\"collation\":\"utf8_general_ci\",\"columnDefault\":\"NULL\",\"unsigned\":false,\"binary\":false,\"pk\":false,\"uk\":false,\"onUpdate\":true}]}";
        assertEquals(data, Codec.DEFAULT.encode(deltaDto));
        assertEquals(deltaDto, Codec.DEFAULT.decode(data, SchemasHistoryDeltaDto.class));
    }

    @Test
    public void emptyList() {
        String data = "{\"schemasHistoryDeltas\": []}";
        SchemasHistoryDto history = new SchemasHistoryDto();
        history.setSchemasHistoryDeltas(Lists.newArrayList());
        assertEquals(history, Codec.DEFAULT.decode(data, SchemasHistoryDto.class));
    }
}