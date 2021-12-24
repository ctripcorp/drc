package com.ctrip.framework.drc.core.driver.schema;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent.Column;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public class FixedRAMSchemasHistoryTest {

    FixedRAMSchemasHistory history;

    @Before
    public void setUp() throws Exception {
        history = new FixedRAMSchemasHistory();
    }

    @After
    public void tearDown() throws Exception {
        history = null;
    }

    @Test
    public void simpleUse() {
        List<Column> input = Lists.newArrayList(
                new Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, "default"),
                new Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null)
        );
        List<Column> another = Lists.newArrayList(
                new Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, "default"),
                new Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null),
                new Column("lt", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null)
        );
        history.merge(new GtidSet(""), TableKey.from("prod", "hello"), Columns.from(input));
        assertTrue(history.isMerged(new GtidSet(""), TableKey.from("prod", "hello"), Columns.from(input)));
        assertFalse(history.isMerged(new GtidSet(""), TableKey.from("prod", "hello1"), Columns.from(input)));
        assertFalse(history.isMerged(new GtidSet(""), TableKey.from("prod", "hello"), Columns.from(another)));
        Columns output = history.query("").getColumns("prod", "hello");
        assertEquals(2, output.size());
        assertSame(input.get(0), output.get(0));
        assertSame(input.get(1), output.get(1));
    }
}