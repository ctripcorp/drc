package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.entity.Db;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2024/12/9 10:51
 */
public class DbTest {

    @Test
    public void testEqual() {
        Db db1 = new Db();
        Db db2 = new Db();

        db1.setMaster(false);
        db1.setUuid(null);
        db1.setPort(55944);
        db1.setIp("ip");

        db2.setMaster(false);
        db2.setUuid(null);
        db2.setPort(55944);
        db2.setIp("ip");
        Assert.assertEquals(db1, db2);
    }
}
