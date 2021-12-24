package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.resource.context.sql.SelectBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/10/27
 */
public class SelectBuilderTest extends SQLUtilTest {

    @Test
    public void prepare() {

        assertEquals("SELECT * FROM db.test WHERE `id`=?",
                new SelectBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(true, false, false))
                        .prepare());

        assertEquals("SELECT * FROM db.test WHERE `id`=? AND `name`=?",
                new SelectBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(true, true, false))
                        .prepare());

        assertEquals("SELECT * FROM db.test WHERE `id`=? AND `name`=? AND `password`=?",
                new SelectBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(true, true, true))
                        .prepare());

    }
}