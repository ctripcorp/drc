package com.ctrip.framework.drc.core.service.email;

/**
 * @ClassName BlankEmailServiceImpl
 * @Author haodongPan
 * @Date 2023/11/23 14:29
 * @Version: $
 */
public class BlankEmailServiceImpl implements EmailService{

    @Override
    public EmailResponse sendEmail(Email email) {
        return null;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
