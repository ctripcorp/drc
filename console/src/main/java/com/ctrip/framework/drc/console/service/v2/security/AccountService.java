package com.ctrip.framework.drc.console.service.v2.security;


import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public interface AccountService {
   
    String decrypt(String passwordToken); 
    
    String encrypt(String password);
    
    MhaAccounts getMhaAccounts(MhaTblV2 mhaTblV2);

    MhaAccounts getMhaAccounts(String mhaName) throws SQLException;

    Account getAccount(MhaTblV2 mhaTblV2, DrcAccountTypeEnum accountType);
    
    void loadEncryptCache() throws SQLException , InterruptedException;
    
    Pair<Boolean,Integer> initMhaPasswordToken(List<String> mhas) throws SQLException;
    
    boolean grayKmsToken(String mhaName);
    
    boolean grayAccountV2(String mhaName);

    /**
     * 
     * @param mhas
     * @param forceChange ignore accountV2 old pwd
     * @return k: success count ,value: fail info
     * @throws SQLException
     */
    Pair<Integer, String> mhaAccountV2ChangePwd(List<String> mhas,boolean forceChange) throws SQLException;

    // auto build drc ,call this api init and change pwd in account v2
    boolean mhaAccountV2ChangeAndRecord(MhaTblV2 mhaTblV2,String masterNodeIp,Integer masterNodePort);
    
    // key: success count , value: error message
    Pair<Integer,String> accountV2Check(List<String> mhas) throws SQLException;
    
}