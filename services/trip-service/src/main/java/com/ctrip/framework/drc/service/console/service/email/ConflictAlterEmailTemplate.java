package com.ctrip.framework.drc.service.console.service.email;

import com.ctrip.framework.drc.core.service.email.Email;

/**
 * @ClassName CflAlterEmailTemplate
 * @Author haodongPan
 * @Date 2023/11/23 15:34
 * @Version: $
 */
public class ConflictAlterEmailTemplate implements TripEmailTemplate {
    public static final int APP_ID = 100023928;
    public static final int BODY_TEMPLATE_ID = 37030167;
    public static final String SEND_CODE = "37030167";
    public static final boolean IS_BODY_HTML = true;
    public static final String XML_FORMATTER = "<entry><content><![CDATA[%s]]></content></entry>";
    
    @Override
    public int getAppID() {
        return APP_ID;
    }

    @Override
    public int getBodyTemplateID() {
        return BODY_TEMPLATE_ID;
    }

    @Override
    public boolean isBodyHtml() {
        return IS_BODY_HTML;
    }

    @Override
    public String getSendCode() {
        return SEND_CODE;
    }

    @Override
    public void formatBodyContent(Email email) {
        if (email == null) return;
        String content = email.getBodyContent();
        content = content.replaceAll("\n", "<br/>");
        content = String.format(XML_FORMATTER, content);
        email.setBodyContent(content);
    }
    
    
}
