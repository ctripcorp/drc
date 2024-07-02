package com.ctrip.framework.drc.console.vo.check.v2;

/**
 * @ClassName AccountPrivilege
 * @Author haodongPan
 * @Date 2024/7/1 17:52
 * @Version: $
 */
public class AccountPrivilege {
    private String acc;
    private String res;

    public AccountPrivilege() {
    }

    public AccountPrivilege(String acc, String res) {
        this.acc = acc;
        this.res = res;
    }

    public String getAcc() {
        return acc;
    }

    public void setAcc(String acc) {
        this.acc = acc;
    }

    public String getRes() {
        return res;
    }

    public void setRes(String res) {
        this.res = res;
    }

    @Override
    public String toString() {
        return "AccountPrivilege{" +
                "acc='" + acc + '\'' +
                ", res='" + res + '\'' +
                '}';
    }
}
