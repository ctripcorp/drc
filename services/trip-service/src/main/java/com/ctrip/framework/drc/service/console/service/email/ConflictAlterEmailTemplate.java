package com.ctrip.framework.drc.service.console.service.email;

import com.ctrip.framework.drc.core.service.email.Email;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
        Map<String, String> contentKeyValues = email.getContentKeyValues();
        if (contentKeyValues == null || contentKeyValues.isEmpty()) return;
        Iterator<Entry<String, String>> iterator = contentKeyValues.entrySet().iterator();
        StringBuilder content = new StringBuilder();
        while (iterator.hasNext()) {
            Entry<String, String> next = iterator.next();
            String key = next.getKey();
            String value = next.getValue();
            content.append(key).append(": ").append(value).append(iterator.hasNext() ? "</br>" : "");
        }
        String bodyContent = String.format(XML_FORMATTER, content);
        email.setBodyContent(bodyContent);
    }
    
    
}
