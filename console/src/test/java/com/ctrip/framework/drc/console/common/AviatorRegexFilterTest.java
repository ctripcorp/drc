package com.ctrip.framework.drc.console.common;

import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/10 21:31
 */
public class AviatorRegexFilterTest {

    @Test
    public void testFilter() {
        String filter = "fncbicardindexdb\\.(uid_brandid_collectionid_rel_\\d*)";
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(filter);
        System.out.println(aviatorRegexFilter.filter("fncbicardindexdb.uid_brandid_collectionid_rel_ss"));
    }

    @Test
    public void testString() {
        List<String> list = Lists.newArrayList("a", "b", "c");
        System.out.println(Joiner.on("|").join(list));
    }
}
