package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.UserContext;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class LocalUidService implements UidService {

    @Override
    public RowsFilterResult.Status filterUid(UserContext uidContext) throws Exception {
        try {
            // 1 3 5 -> 1 5 AbstractRowsFilterRuleForUidTest
            int value = Integer.parseInt(uidContext.getUserAttr());
            if (value < 10) {
                return RowsFilterResult.Status.from(value % 4 == 1);
            }
            return RowsFilterResult.Status.from(true);
        } catch (Exception e) {
            return RowsFilterResult.Status.Illegal;
        }
    }

    @Override
    public RowsFilterResult.Status filterUdl(UserContext uidContext) throws Exception {
        try {
            // 2 4 6 -> 2(degrade by uid=1) 6  AbstractRowsFilterRuleForUdlTest
            int value = Integer.parseInt(uidContext.getUserAttr());
            if (value == 2) {
                return RowsFilterResult.Status.Illegal;
            }
            return RowsFilterResult.Status.from(Integer.parseInt(uidContext.getUserAttr()) % 3 == 0);
        } catch (Exception e) {
            return RowsFilterResult.Status.Illegal;
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
