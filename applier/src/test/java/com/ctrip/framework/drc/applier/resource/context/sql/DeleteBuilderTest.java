package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.resource.context.sql.DeleteBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/10/27
 */
public class DeleteBuilderTest extends SQLUtilTest {

    @Test
    public void prepare() {

        assertEquals("DELETE FROM db.test WHERE `id`=?",
                new DeleteBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(true, false, false))
                        .prepare());

        assertEquals("DELETE FROM db.test WHERE `id`=? AND `name`=?",
                new DeleteBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(true, true, false))
                        .prepare());

        assertEquals("DELETE FROM db.test WHERE `id`=? AND `name`=? AND `password`=?",
                new DeleteBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(true, true, true))
                        .prepare());

    }
}