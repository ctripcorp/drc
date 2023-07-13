package com.ctrip.framework.drc.console.common;

import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import org.junit.Test;

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
}
