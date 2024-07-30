package com.ctrip.framework.drc.core.service.security;

/**
 * @ClassName BlankHeraldService
 * @Author haodongPan
 * @Date 2024/5/31 10:43
 * @Version: $
 */
public class BlankHeraldServiceImpl implements HeraldService {

    @Override
    public String getLocalHeraldToken() {
        return null;
    }

    @Override
    public boolean heraldValidate(String heraldToken) {
        return false;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
