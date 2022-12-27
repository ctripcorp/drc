package com.ctrip.framework.drc.console.schedule;

/**
 * Created by jixinwang on 2022/12/14
 */
public class ProviderContext {

    private String cloud_provider;
    private String account_id;
    private String account_name;

    public ProviderContext(String cloud_provider, String account_id, String account_name) {
        this.cloud_provider = cloud_provider;
        this.account_id = account_id;
        this.account_name = account_name;
    }

    public String getCloud_provider() {
        return cloud_provider;
    }

    public void setCloud_provider(String cloud_provider) {
        this.cloud_provider = cloud_provider;
    }

    public String getAccount_id() {
        return account_id;
    }

    public void setAccount_id(String account_id) {
        this.account_id = account_id;
    }

    public String getAccount_name() {
        return account_name;
    }

    public void setAccount_name(String account_name) {
        this.account_name = account_name;
    }
}
