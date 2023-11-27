package com.ctrip.framework.drc.core.service.email;


import com.ctrip.xpipe.api.lifecycle.Ordered;

public interface EmailService extends Ordered {
    
    EmailResponse sendEmail(Email email);

}
