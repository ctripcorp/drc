package com.ctrip.framework.drc.console.param.v2.security;

/**
 * @ClassName MhaAccount
 * @Author haodongPan
 * @Date 2024/6/3 17:06
 * @Version: $
 */
public class MhaAccounts {
    private String mha;
    
    private Account monitorAcc;
    
    private Account readAcc;
    
    private Account writeAcc;
    
    public MhaAccounts() {
    }
    
    public MhaAccounts(String mha, Account monitorAcc, Account readAcc, Account writeAcc) {
        this.mha = mha;
        this.monitorAcc = monitorAcc;
        this.readAcc = readAcc;
        this.writeAcc = writeAcc;
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }

    public Account getMonitorAcc() {
        return monitorAcc;
    }

    public void setMonitorAcc(Account monitorAcc) {
        this.monitorAcc = monitorAcc;
    }

    public Account getReadAcc() {
        return readAcc;
    }

    public void setReadAcc(Account readAcc) {
        this.readAcc = readAcc;
    }

    public Account getWriteAcc() {
        return writeAcc;
    }

    public void setWriteAcc(Account writeAcc) {
        this.writeAcc = writeAcc;
    }
}
