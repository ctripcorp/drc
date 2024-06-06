package com.ctrip.framework.drc.service.security;

import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.sysdev.herald.tokenlib.Token;

/**
 * @ClassName TripHeraldServiceImpl
 * @Author haodongPan
 * @Date 2024/5/31 11:03
 * @Version: $
 */
public class TripHeraldServiceImpl implements HeraldService {

    @Override
    public String getLocalHeraldToken() {
        return Token.getTokenString();
    }

    @Override
    public boolean heraldValidate(String heraldToken) {
        // todo hdpan
        return false;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
