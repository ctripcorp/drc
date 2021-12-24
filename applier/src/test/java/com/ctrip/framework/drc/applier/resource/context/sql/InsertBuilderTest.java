package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.resource.context.sql.InsertBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 09, 2019
 */
public class InsertBuilderTest extends SQLUtilTest {

    @Test
    public void prepare() {

        assertEquals("INSERT INTO db.test (`name`,`password`) VALUES (?,?)",
                new InsertBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(false, true, true))
                        .prepareSingle());

        assertEquals("INSERT INTO db.test (`name`,`password`) VALUES (?,?)",
                new InsertBuilder("db.test",
                        buildArray("id", "name", "password"),
                        buildArray(false, true, true))
                        .prepareSingle());

    }
}