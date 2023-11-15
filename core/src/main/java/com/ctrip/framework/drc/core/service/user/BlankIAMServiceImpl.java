package com.ctrip.framework.drc.core.service.user;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @ClassName BlankIAMServiceImpl
 * @Author haodongPan
 * @Date 2023/11/14 11:32
 * @Version: $
 */
public class BlankIAMServiceImpl implements IAMService {
    

    @Override
    public Pair<Boolean, String> canQueryAllCflLog() {
        return null;
    }

    @Override
    public Pair<Boolean, String> checkPermission(List<String> permissionCodes, String eid) {
        return null;
    }

    @Override
    public String matchApiPermissionCode(String requestURL) {
        return null;
    }

    @Override
    public boolean iamFilterEnable() {
        return false;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
