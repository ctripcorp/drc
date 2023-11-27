package com.ctrip.framework.drc.core.service.email;

import java.util.LinkedList;
import java.util.List;

/**
 * @ClassName Email
 * @Author haodongPan
 * @Date 2023/11/23 14:27
 * @Version: $
 */
public class Email {
    private List<String> recipients;
    private List<String> cc;
    private List<String> bcc;
    private String sender;
    private String subject;
    private String bodyContent;

    public Email() {
        recipients = new LinkedList<>();
        cc = new LinkedList<>();
        bcc = new LinkedList<>();
    }
    
    
    public void addRecipient(String emailAddr) {
        recipients.add(emailAddr);
    }
    
    public void addCc(String emailAddr) {
        cc.add(emailAddr);
    }
    
    public void addBcc(String emailAddr) {
        bcc.add(emailAddr);
    }

    public List<String> getRecipients() {
        return recipients;
    }

    public void setRecipients(List<String> recipients) {
        this.recipients = recipients;
    }

    public List<String> getCc() {
        return cc;
    }

    public void setCc(List<String> cc) {
        this.cc = cc;
    }

    public List<String> getBcc() {
        return bcc;
    }

    public void setBcc(List<String> bcc) {
        this.bcc = bcc;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getBodyContent() {
        return bodyContent;
    }

    public void setBodyContent(String bodyContent) {
        this.bodyContent = bodyContent;
    }

    @Override
    public String toString() {
        return "Email{" +
                "recipients=" + recipients +
                ", cc=" + cc +
                ", bcc=" + bcc +
                ", sender='" + sender + '\'' +
                ", subject='" + subject + '\'' +
                ", bodyContent='" + bodyContent + '\'' +
                '}';
    }
}
