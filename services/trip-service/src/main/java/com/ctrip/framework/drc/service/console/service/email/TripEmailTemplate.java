package com.ctrip.framework.drc.service.console.service.email;

import com.ctrip.framework.drc.core.service.email.Email;

/**
 * @ClassName TripEmailTemplate
 * @Author haodongPan
 * @Date 2023/11/23 15:24
 * @Version: $
 */
public interface TripEmailTemplate {
    
    int getAppID();
    int getBodyTemplateID();
    boolean isBodyHtml();
    String getSendCode();
    void formatBodyContent(Email email);
}
