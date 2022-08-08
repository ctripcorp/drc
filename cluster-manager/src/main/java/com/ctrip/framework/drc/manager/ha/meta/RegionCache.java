package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.DbCluster;

/**
 * Created by jixinwang on 2022/8/2
 */
public interface RegionCache extends DcCache {

    void clusterAdded(String dcId, DbCluster dbCluster);
}
