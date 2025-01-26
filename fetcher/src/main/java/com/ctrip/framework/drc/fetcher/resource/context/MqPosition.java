package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;

/**
 * Created by jixinwang on 2022/10/24
 */
public interface MqPosition {

    void add(Gtid gtid);

    void union(GtidSet gtidSet);

    String get();

    void release() throws Exception;
}
