package com.ctrip.framework.drc.core.server.common.filter.row;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/9/26
 */
public enum Region {
    
    SGP(Sets.newHashSet("SGP","SIN")), 

    FRA(Sets.newHashSet("FRA")),

    SHA(Sets.newHashSet("SHA", "SH"));

    private Set<String> names;

    Region(Set<String> names) {
        this.names = names;
    }
    
    public boolean isLegal(String region) {
        return names.contains(region);
    }
    public static Region nameFor(String name) {
        for (Region region : values()) {
            if (region.names.contains(name)) {
                return region;
            }
        }

        throw new UnsupportedOperationException("not support for name " + name);
    }
}
