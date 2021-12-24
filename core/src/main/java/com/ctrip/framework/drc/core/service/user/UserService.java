package com.ctrip.framework.drc.core.service.user;

import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * @author wangjixin
 * @version 1.0
 * date: 2020-03-03
 */
public interface UserService extends Ordered {
    String getInfo();
    String getLogoutUrl();
}
