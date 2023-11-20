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
        String filter = "(?!bbzmbrcommoncontactshard0[1-2]db|CommonOrderShard[1-9]DB|CommonOrderShard[1][0-2]DB).*\\.*";
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(filter);
        System.out.println(aviatorRegexFilter.filter("commonorderconfigdb.xjob_timestamp"));
    }

    @Test
    public void testString() {
        List<String> list = Lists.newArrayList("a", "b", "c");
        System.out.println(Joiner.on("|").join(list));
    }
}
