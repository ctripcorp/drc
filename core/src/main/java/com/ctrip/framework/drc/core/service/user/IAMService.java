package com.ctrip.framework.drc.core.service.user;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public interface IAMService extends Ordered {

    // Left: res, Right: msg
    Pair<Boolean,String> checkPermission(List<String> permissionCodes, String eid);
    
    String matchApiPermissionCode(String requestURL);

    boolean iamFilterEnable();

    Pair<Boolean,String> canQueryAllDbReplication();
}
