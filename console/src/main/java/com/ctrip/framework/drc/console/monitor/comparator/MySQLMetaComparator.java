package com.ctrip.framework.drc.console.monitor.comparator;

import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;

/**
 * Created by jixinwang on 2021/7/30
 */
public class MySQLMetaComparator extends AbstractMetaComparator<MetaKey, MySQLEndpointChange> {

    private MetaKey metaKey;

    public MySQLMetaComparator(MetaKey metaKey) {
        this.metaKey = metaKey;
    }

    public MetaKey getMetaKey() {
        return metaKey;
    }

    @Override
    public void compare() {

    }

    @Override
    public String idDesc() {
        return null;
    }
}
