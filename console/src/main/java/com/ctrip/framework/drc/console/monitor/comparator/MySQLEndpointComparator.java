package com.ctrip.framework.drc.console.monitor.comparator;

import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.google.common.collect.Sets;
import org.unidal.tuple.Triple;

import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2021/7/30
 */
public class MySQLEndpointComparator extends AbstractMetaComparator<MetaKey, MySQLEndpointChange> {

    private Map<MetaKey, MySqlEndpoint> current, future;

    public MySQLEndpointComparator(Map<MetaKey, MySqlEndpoint> current, Map<MetaKey, MySqlEndpoint> future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        Triple<Set<MetaKey>, Set<MetaKey>, Set<MetaKey>> result = getDiff(Sets.newHashSet(current.keySet()),
                Sets.newHashSet(future.keySet()));

        added = result.getFirst();
        removed = result.getLast();
        Set<MetaKey> intersectionMetaKey = result.getMiddle();

        for(MetaKey metaKey : intersectionMetaKey) {
            MySqlEndpoint currentMySqlEndpoint = current.get(metaKey);
            MySqlEndpoint futureMySqlEndpoint = future.get(metaKey);
            if(currentMySqlEndpoint == null || futureMySqlEndpoint == null) {
                modified.add(new MySQLMetaComparator(metaKey));
                continue;
            }
            if(!currentMySqlEndpoint.equalsWithUserAndPassword(futureMySqlEndpoint)) {
                modified.add(new MySQLMetaComparator(metaKey));
            }
        }
    }

    @Override
    public String idDesc() {
        return null;
    }
}
