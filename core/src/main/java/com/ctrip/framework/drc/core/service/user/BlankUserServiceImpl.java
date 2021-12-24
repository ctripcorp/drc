package com.ctrip.framework.drc.core.service.user;

/**
 * @ClassName BlankUserServiceImpl
 * @Author haodongPan
 * @Date 2021/12/6 17:48
 * @Version: $
 */
public class BlankUserServiceImpl implements UserService {
    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public String getLogoutUrl() {
        return null;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
