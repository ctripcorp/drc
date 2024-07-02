package com.ctrip.framework.drc.console.vo.check.v2;

import java.util.List;

/**
 * @ClassName AccountPrivilegeCheckVo
 * @Author haodongPan
 * @Date 2024/7/1 17:40
 * @Version: $
 */
public class AccountPrivilegeCheckVo {
    
    private String mhaName;
    private List<AccountPrivilege> accounts;
    private String msg;

    

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<AccountPrivilege> getAccounts() {
        return accounts;
    }

    public void setAccounts(List<AccountPrivilege> accounts) {
        this.accounts = accounts;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "AccountPrivilegeCheckVo{" +
                "mhaName='" + mhaName + '\'' +
                ", accounts=" + accounts +
                ", msg='" + msg + '\'' +
                '}';
    }
}
