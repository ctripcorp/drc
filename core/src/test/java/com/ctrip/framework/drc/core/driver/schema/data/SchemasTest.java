package com.ctrip.framework.drc.core.driver.schema.data;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent.Column;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Oct 12, 2019
 */
public class SchemasTest {

    Schemas schemas;

    @Before
    public void setUp() throws Exception {
        schemas = Schemas.from(new HashMap<TableKey, Columns>() {{
            put(TableKey.from("prod", "hello"), Columns.from(Lists.newArrayList(
                    new Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, null),
                    new Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null)
            )));
        }});
    }

    @After
    public void tearDown() throws Exception {
        schemas = null;
    }

    @Test
    public void testGetColumns() {
        Columns columns = schemas.getColumns("prod", "hello");
        List<String> expectedColumnNames = Lists.newArrayList("id", "user");
        List<String> columnNames = columns.getNames();
        assertEquals(2, columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            assertEquals(expectedColumnNames.get(i), columnNames.get(i));
        }
    }

    @Test
    public void testMergeSingle() {
        Schemas merged = schemas.merge(TableKey.from("prod", "hello"),
                Columns.from(Lists.newArrayList(
                        new Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, null),
                        new Column("company", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null)
        )));
        assertEquals("[id, company]", merged.getColumns("prod", "hello").getNames().toString());
    }

    @Test
    public void testMergeSchemas() {
        Schemas merged = schemas.merge(new HashMap<TableKey, Columns>() {{
            put(TableKey.from("prod", "hello"), Columns.from(Lists.newArrayList(
                    new Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, null),
                    new Column("company", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null)
            )));
        }});
        assertNotSame(merged, schemas);
        assertEquals("[id, company]", merged.getColumns("prod", "hello").getNames().toString());
    }

    @Test
    public void testClone() {
        Schemas cloned = schemas.clone();
        assertNotSame(cloned, schemas);
        assertEquals(
                cloned.getColumns("prod", "hello").getNames().toString(),
                schemas.getColumns("prod", "hello").getNames().toString());
    }

    @Test
    public void testEmpty() {
        Schemas empty = Schemas.empty();
        assertEquals(0, empty.keySet().size());
    }
}