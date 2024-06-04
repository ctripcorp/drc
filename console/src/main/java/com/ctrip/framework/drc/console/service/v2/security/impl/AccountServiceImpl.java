package com.ctrip.framework.drc.console.service.v2.security.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.service.v2.security.DataSourceCrypto;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName AccountServiceImpl
 * @Author haodongPan
 * @Date 2024/3/25 19:20
 * @Version: $
 */
@Service
public class AccountServiceImpl implements AccountService {
    
    private final Map<String,String> decryptCache = new ConcurrentHashMap<>();
    private final Map<String,String> encryptCache = new ConcurrentHashMap<>();
    
    @Autowired
    private KmsService kmsService;
    @Autowired 
    private DataSourceCrypto dataSourceCrypto;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DbaApiService dbaApiService;
    private TransactionMonitor transactionMonitor = DefaultTransactionMonitorHolder.getInstance();
    
    private Logger logger = LoggerFactory.getLogger(AccountServiceImpl.class);
    
    @PostConstruct
    public void init() {
        try {
            if (consoleConfig.isCenterRegion()) {
                loadEncryptCache();
            }
        } catch (SQLException e) {
            logger.error("init loadEncryptCache failed",e);
            throw ConsoleExceptionUtils.message("loadCache failed");
        }
    }
    

    @Override
    public String decrypt(String passwordToken) {
        if (decryptCache.containsKey(passwordToken)) {
            return decryptCache.get(passwordToken);
        }
        String password = new RetryTask<String>(() -> doDecrypt(passwordToken), 2).call();
        if (StringUtils.isEmpty(password)) {
            throw ConsoleExceptionUtils.message("doDecrypt password failed");
        }
        decryptCache.put(passwordToken,password);
        return password;
    }
    
    @Override
    public String encrypt(String password) {
        if (encryptCache.containsKey(password)) {
            return encryptCache.get(password);
        }
        String passwordToken = new RetryTask<String>(() -> doEncrypt(password), 2).call();
        if (StringUtils.isEmpty(passwordToken)) {
            throw ConsoleExceptionUtils.message("doEncrypt password failed");
        }
        encryptCache.put(password,passwordToken);
        return passwordToken;
    }

    @Override
    public MhaAccounts getMhaAccounts(MhaTblV2 mhaTblV2) {
        Account monitorAcc = new Account(mhaTblV2.getMonitorUser(), decrypt(mhaTblV2.getMonitorPasswordToken()));
        Account readAcc = new Account(mhaTblV2.getReadUser(), decrypt(mhaTblV2.getReadPasswordToken()));
        Account writeAcc = new Account(mhaTblV2.getWriteUser(), decrypt(mhaTblV2.getWritePasswordToken()));
        return new MhaAccounts(mhaTblV2.getMhaName(),monitorAcc,readAcc,writeAcc);
    }

    @Override
    public Account getAccount(MhaTblV2 mhaTblV2, DrcAccountTypeEnum accountType) {
        switch (accountType) {
            case DRC_CONSOLE:
                return new Account(mhaTblV2.getMonitorUser(),decrypt(mhaTblV2.getMonitorPasswordToken()));
            case DRC_READ:
                return new Account(mhaTblV2.getReadUser(),decrypt(mhaTblV2.getReadPasswordToken()));
            case DRC_WRITE:
                return new Account(mhaTblV2.getWriteUser(),decrypt(mhaTblV2.getWritePasswordToken()));
            default:
                throw ConsoleExceptionUtils.message("accountType not support");
        }
    }

    @Override
    public void loadEncryptCache() throws SQLException {
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        Set<MhaTblV2> mhaGrayKmsToken = mhaTblV2s.stream().filter(mhaTblV2 -> grayKmsToken(mhaTblV2.getMhaName()))
                .collect(Collectors.toSet());
        for (MhaTblV2 mhaTblV2 : mhaGrayKmsToken) {
            String monitorPasswordToken = mhaTblV2.getMonitorPasswordToken();
            if (decryptCache.containsKey(monitorPasswordToken)) {
                continue;
            }
            String monitorPassword = decrypt(monitorPasswordToken);
            decryptCache.put(monitorPasswordToken,monitorPassword);

            String readPasswordToken = mhaTblV2.getReadPasswordToken();
            if (decryptCache.containsKey(readPasswordToken)) {
                continue;
            }
            String readPassword = decrypt(readPasswordToken);
            decryptCache.put(readPasswordToken,readPassword);
            
            String writePasswordToken = mhaTblV2.getWritePasswordToken();
            if (decryptCache.containsKey(writePasswordToken)) {
                continue;
            }
            String writePassword = decrypt(writePasswordToken);
            decryptCache.put(writePasswordToken,writePassword);
        }
    }
    
    @Override
    public Pair<Boolean, Integer> initMhaPasswordToken(List<String> mhas) throws SQLException {
        int successCount = 0;
        for (String mha : mhas) {
            if (initMhaPasswordToken(mha)) {
                successCount++;
            }
        }
        return Pair.of(successCount == mhas.size(), successCount);
    }

    @Override
    public boolean grayKmsToken(String mhaName) {
        boolean accountKmsTokenSwitch = consoleConfig.getAccountKmsTokenSwitch();
        if (!accountKmsTokenSwitch) {
            return false;
        }
        Set<String> accountKmsTokenMhaGray = consoleConfig.getAccountKmsTokenMhaGray();
        return accountKmsTokenMhaGray.contains("*") || accountKmsTokenMhaGray.contains(mhaName);
    }
    
    @Override
    public Pair<Boolean,Integer> initMhaAccountV2(List<String> mhas) throws SQLException {
        return null;
    }

    @Override
    public Pair<Boolean,String> accountV2Check(List<String> mhas) {
        // test connection & authority 3 account
        return null;
    }

    @Override
    public boolean grayAccountV2(String mhaName) {
        return false;
    }
    
    @Override
    public boolean changePasswordInNewAccount(String mha, String user, String newPassword){
        return true;
    }
    
    private boolean initMhaPasswordToken(String mha) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mha, 0);
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(mha + "not exist");
        }
        if (!StringUtils.isBlank(mhaTblV2.getMonitorPasswordToken()) 
                && !StringUtils.isBlank(mhaTblV2.getReadPasswordToken())
                && !StringUtils.isBlank(mhaTblV2.getWritePasswordToken())) {
            return true;
        }
        String monitorPasswordToken = encrypt(mhaTblV2.getMonitorPassword());
        String readPasswordToken = encrypt(mhaTblV2.getReadPassword());
        String writePasswordToken = encrypt(mhaTblV2.getWritePassword());
        mhaTblV2.setMonitorPasswordToken(monitorPasswordToken);
        mhaTblV2.setReadPasswordToken(readPasswordToken);
        mhaTblV2.setWritePasswordToken(writePasswordToken);
        return mhaTblV2Dao.update(mhaTblV2) == 1;
    }
    
    
    private boolean accountV2Check(String mha) throws SQLException {
        return false; // todo hdpan  

    }

    private boolean initMhaAccountV2(String mha) throws SQLException {
        return false; // todo hdpan
    }
    
    

    private String generateNewPassword(String mhaName,String defaultPassword) {
        return null; // todo hdpan
    }
    
    private String doEncrypt(String password) throws Exception {
        return transactionMonitor.logTransaction(
                "DRC.console.account.encrypt", 
                "encrypt password",
                () -> {
                    String secretKey = kmsService.getSecretKey(consoleConfig.getKMSAccessToken("account"));
                    return dataSourceCrypto.encrypt(password, secretKey);
            }
        );
    }

    private String doDecrypt(String passwordToken) throws Exception {
        return transactionMonitor.logTransaction(
                "DRC.console.account.decrypt", 
                "decrypt password" + passwordToken,
                () -> {
                    String secretKey = kmsService.getSecretKey(consoleConfig.getKMSAccessToken("account"));
                    return dataSourceCrypto.decrypt(passwordToken, secretKey);
                }
        );
    }

}
