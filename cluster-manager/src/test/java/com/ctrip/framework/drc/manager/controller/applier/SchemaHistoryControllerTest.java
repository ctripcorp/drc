package com.ctrip.framework.drc.manager.controller.applier;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDeltaDto;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

/**
 * @Author Slight
 * Nov 19, 2019
 */
public class SchemaHistoryControllerTest {

    Columns columns = Columns.from(Lists.newArrayList(
            new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", "PRI", null, "NULL"),
            new TableMapLogEvent.Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, "NULL"),
            new TableMapLogEvent.Column("gender", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, "on update", "NULL")
    ));

    @Test
    public void add() {
        RestTemplate template = new RestTemplate();
        SchemasHistoryDeltaDto delta = new SchemasHistoryDeltaDto();
        delta.setTable("");
        delta.setGtidExecuted("");
        delta.setColumns(columns);
        try {
            template.put("http://127.0.0.1:8080/applier/1/schema", delta);
        } catch (Throwable t) {
            System.out.println("fail to upload schema");
        }
    }
}