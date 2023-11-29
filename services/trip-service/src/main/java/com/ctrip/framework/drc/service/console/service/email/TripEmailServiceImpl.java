package com.ctrip.framework.drc.service.console.service.email;

import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.soa.platform.basesystem.emailservice.v1.EmailServiceClient;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailRequest;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailResponse;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName CtripEmailServiceImpl
 * @Author haodongPan
 * @Date 2023/11/23 14:30
 * @Version: $
 */
public class TripEmailServiceImpl implements EmailService {
    
    private EmailServiceClient client = EmailServiceClient.getInstance();
    private Logger logger = LoggerFactory.getLogger(TripEmailServiceImpl.class);

    @Override
    public EmailResponse sendEmail(Email email) {
        SendEmailRequest sendEmailRequest = createSendEmailRequest(email);
        EmailResponse emailResponse = new EmailResponse();
        try {
            SendEmailResponse sendEmailResponse = client.sendEmail(sendEmailRequest);
            emailResponse.setSuccess(sendEmailResponse.getResultCode() == 1);
            emailResponse.setMessage(sendEmailResponse.getResultMsg());
            emailResponse.setEmailIdList(sendEmailResponse.getEmailIDList());
        } catch (Exception e) {
            emailResponse.setSuccess(false);
            emailResponse.setMessage(e.getMessage());
            logger.warn("send email failed, email: {}", email, e);
        }
        return emailResponse;
    }

    private static SendEmailRequest createSendEmailRequest(Email email) {
        ConflictAlterEmailTemplate template = new ConflictAlterEmailTemplate();
        template.formatBodyContent(email);
        SendEmailRequest request = new SendEmailRequest();
        request.setSendCode(template.getSendCode());
        request.setAppID(template.getAppID());
        request.setCc(CollectionUtils.isEmpty(email.getCc()) ? null : email.getCc());
        request.setBcc(CollectionUtils.isEmpty(email.getBcc()) ? null : email.getBcc());
        request.setSender(email.getSender());
        request.setRecipient(email.getRecipients());
        request.setSubject(email.getSubject());
        request.setBodyContent(email.getBodyContent()); 
        request.setBodyTemplateID(template.getBodyTemplateID());
        request.setIsBodyHtml(template.isBodyHtml());
        return request;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
