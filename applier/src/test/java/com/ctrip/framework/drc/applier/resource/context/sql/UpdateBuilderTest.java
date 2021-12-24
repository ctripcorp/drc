package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.resource.context.sql.UpdateBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 17, 2019
 */
public class UpdateBuilderTest extends SQLUtilTest {

    @Test
    public void prepare() {

        assertEquals("UPDATE prod.hello SET `id`=?,`user`=? WHERE `id`=? AND `user`=?", new UpdateBuilder(
                "prod.hello",
                buildArray("id", "user"),
                buildArray(true, true),
                buildArray(true, true)).prepare());

        assertEquals("UPDATE `prod`.`hello` SET `id`=?,`user`=?,`lt`=? WHERE `id`=? AND `lt`<=?", new UpdateBuilder(
                "`prod`.`hello`",
                buildArray("id", "user", "lt"),
                buildArray(true, true, true),
                buildArray(true, false, false),
                buildArray(false, false, true)
                ).prepare());

        assertEquals("UPDATE `prod`.`hello` SET `id`=?,`user`=?,`lt`=? WHERE `id`=? AND `user`=? AND `lt`<=?", new UpdateBuilder(
                "`prod`.`hello`",
                buildArray("id", "user", "lt"),
                buildArray(true, true, true),
                buildArray(true, true, false),
                buildArray(false, false, true)
        ).prepare());
    }
}