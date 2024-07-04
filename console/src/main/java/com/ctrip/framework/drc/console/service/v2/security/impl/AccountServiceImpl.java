package com.ctrip.framework.drc.console.service.v2.security.impl;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.service.v2.security.DataSourceCrypto;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.check.v2.AccountPrivilege;
import com.ctrip.framework.drc.console.vo.check.v2.AccountPrivilegeCheckVo;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName AccountServiceImpl
 * @Author haodongPan
 * @Date 2024/3/25 19:20
 * @Version: $
 */
@Service
public class AccountServiceImpl implements AccountService {
    
    protected final Map<String,String> decryptCache = new ConcurrentHashMap<>(1000);
    protected final Map<String,String> encryptCache = new ConcurrentHashMap<>(1000);

    private ExecutorService accountExecutorService = ThreadUtils.newThreadExecutor(PROCESSORS_SIZE,PROCESSORS_SIZE * 5, "accountService");

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
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    
    private TransactionMonitor transactionMonitor = DefaultTransactionMonitorHolder.getInstance();
    
    private Logger logger = LoggerFactory.getLogger(AccountServiceImpl.class);
    
    @PostConstruct
    public void init() {
        try {
            if (consoleConfig.isCenterRegion()) {
                DefaultTransactionMonitorHolder.getInstance().logTransaction(
                        "Drc.console.loadEncryptCache",
                        "init",
                        (Task) this::loadEncryptCache
                    );
            }
        } catch (Exception e ) {
            logger.error("init loadEncryptCache failed",e);
            throw ConsoleExceptionUtils.message("loadCache failed");
        }
    }
    

    @Override
    public String decrypt(String passwordToken) {
        if (StringUtils.isBlank(passwordToken)) {
            throw ConsoleExceptionUtils.message("passwordToken is empty");
        }
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
        if(StringUtils.isBlank(password)) {
            throw ConsoleExceptionUtils.message("password is empty");
        }
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
        Account monitorAcc = getAccount(mhaTblV2, DrcAccountTypeEnum.DRC_CONSOLE);
        Account readAcc = getAccount(mhaTblV2, DrcAccountTypeEnum.DRC_READ);
        Account writeAcc = getAccount(mhaTblV2, DrcAccountTypeEnum.DRC_WRITE);
        return new MhaAccounts(mhaTblV2.getMhaName(),monitorAcc,readAcc,writeAcc);
    }

    @Override
    public MhaAccounts getMhaAccounts(String mhaName) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, 0);
        return this.getMhaAccounts(mhaTblV2);
    }

    @Override
    public Account getAccount(MhaTblV2 mhaTblV2, DrcAccountTypeEnum accountType) {
        boolean grayKms = grayKmsToken(mhaTblV2.getMhaName());
        boolean grayNewAccount = grayAccountV2(mhaTblV2.getMhaName());
        switch (accountType) {
            case DRC_CONSOLE:
                return grayKms ? 
                        (grayNewAccount ? 
                                new Account(mhaTblV2.getMonitorUserV2(),decrypt(mhaTblV2.getMonitorPasswordTokenV2())) : 
                                new Account(mhaTblV2.getMonitorUser(),decrypt(mhaTblV2.getMonitorPasswordToken()))
                        ) : 
                        new Account(mhaTblV2.getMonitorUser(),mhaTblV2.getMonitorPassword());
            case DRC_READ:
                return grayKms ?
                        (grayNewAccount ?
                                new Account(mhaTblV2.getReadUserV2(),decrypt(mhaTblV2.getReadPasswordTokenV2())) :
                                new Account(mhaTblV2.getReadUser(),decrypt(mhaTblV2.getReadPasswordToken()))
                        ) :
                        new Account(mhaTblV2.getReadUser(),mhaTblV2.getReadPassword());
            case DRC_WRITE:
                return grayKms ?
                        (grayNewAccount ?
                                new Account(mhaTblV2.getWriteUserV2(),decrypt(mhaTblV2.getWritePasswordTokenV2())) :
                                new Account(mhaTblV2.getWriteUser(),decrypt(mhaTblV2.getWritePasswordToken()))
                        ) :
                        new Account(mhaTblV2.getWriteUser(),mhaTblV2.getWritePassword());
            default:
                throw ConsoleExceptionUtils.message("accountType not support");
        }
    }

    @Override
    public void loadEncryptCache() throws SQLException, InterruptedException {
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        CountDownLatch latch = new CountDownLatch(mhaTblV2s.size());
        for (MhaTblV2 mhaTblV2 : mhaTblV2s) {
             accountExecutorService.submit(
                     () -> {
                         loadEncryptCache(mhaTblV2);
                         logger.info("mha:{},loadEncryptCache success",mhaTblV2.getMhaName());
                         latch.countDown();
                     }
             );
        }
        boolean await = latch.await(10, TimeUnit.SECONDS);
        logger.info("loadEncryptCache finish,res:{}",await);
    }
    
    private void loadEncryptCache(MhaTblV2 mhaTblV2) {
        if (grayKmsToken(mhaTblV2.getMhaName())) {
            String monitorPasswordToken = mhaTblV2.getMonitorPasswordToken();
            String readPasswordToken = mhaTblV2.getReadPasswordToken();
            String writePasswordToken = mhaTblV2.getWritePasswordToken();
            if (!decryptCache.containsKey(monitorPasswordToken)) {
                decryptCache.put(monitorPasswordToken, decrypt(monitorPasswordToken));
            }
            if (!decryptCache.containsKey(readPasswordToken)) {
                decryptCache.put(readPasswordToken, decrypt(readPasswordToken));
            }
            if (!decryptCache.containsKey(writePasswordToken)) {
                decryptCache.put(writePasswordToken, decrypt(writePasswordToken));
            }
        }
        if (grayAccountV2(mhaTblV2.getMhaName())) {
            String monitorPasswordTokenV2 = mhaTblV2.getMonitorPasswordTokenV2();
            String readPasswordTokenV2 = mhaTblV2.getReadPasswordTokenV2();
            String writePasswordTokenV2 = mhaTblV2.getWritePasswordTokenV2();
            if (!decryptCache.containsKey(monitorPasswordTokenV2)) {
                decryptCache.put(monitorPasswordTokenV2,decrypt(monitorPasswordTokenV2));
            }
            if (!decryptCache.containsKey(readPasswordTokenV2)) {
                decryptCache.put(readPasswordTokenV2,decrypt(readPasswordTokenV2));
            }
            if (!decryptCache.containsKey(writePasswordTokenV2)) {
                decryptCache.put(writePasswordTokenV2,decrypt(writePasswordTokenV2));
            }
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
    public boolean grayAccountV2(String mhaName) {
        boolean accountKmsTokenSwitch = consoleConfig.getAccountKmsTokenSwitchV2();
        if (!accountKmsTokenSwitch) {
            return false;
        }
        Set<String> accountKmsTokenMhaGray = consoleConfig.getAccountKmsTokenMhaGrayV2();
        return accountKmsTokenMhaGray.contains("*") || accountKmsTokenMhaGray.contains(mhaName);
    }

    @Override
    public Pair<Integer, String> mhaAccountV2ChangePwd(List<String> mhas, boolean forceChange) throws SQLException {
        int successCount = 0;
        StringBuilder msg = new StringBuilder();
        Map<String,Future<Boolean>> futureMap = Maps.newHashMap();
        for (String mha : mhas) {
            Future<Boolean> res = accountExecutorService.submit(() -> mhaAccountV2ChangePwd(mha,forceChange));
            futureMap.put(mha,res);
        }       
        
        for (Map.Entry<String,Future<Boolean>> entry : futureMap.entrySet()) {
            try {
                if (entry.getValue().get(10,TimeUnit.SECONDS)) {
                    successCount++;
                }
            } catch (TimeoutException e ){
                logger.error("initMhaAccountV2 time out",e);
                msg.append(entry.getKey()).append("time out ").append(e.getMessage()).append("\n");
            } catch (Exception e) {
                logger.error("initMhaAccountV2 error",e);
                msg.append(entry.getKey()).append("fail ").append(e.getMessage()).append("\n");
            }
        }
        return Pair.of(successCount, msg.toString());
    }

    @Override
    public boolean mhaAccountV2ChangeAndRecord(MhaTblV2 mhaTblV2, String masterNodeIp, Integer masterNodePort) {
        MhaAccounts mhaAccounts = dbaApiService.accountV2PwdChange(mhaTblV2.getMhaName(), masterNodeIp, masterNodePort);
        if (mhaAccounts == null) {
            return false;
        }
        mhaTblV2.setMonitorUserV2(mhaAccounts.getMonitorAcc().getUser());
        mhaTblV2.setMonitorPasswordTokenV2(this.encrypt(mhaAccounts.getMonitorAcc().getPassword()));
        mhaTblV2.setReadUserV2(mhaAccounts.getReadAcc().getUser());
        mhaTblV2.setReadPasswordTokenV2(this.encrypt(mhaAccounts.getReadAcc().getPassword()));
        mhaTblV2.setWriteUserV2(mhaAccounts.getWriteAcc().getUser());
        mhaTblV2.setWritePasswordTokenV2(this.encrypt(mhaAccounts.getWriteAcc().getPassword()));
        return true;
    }
    

    @Override
    public Pair<Integer, String> accountV2Check(List<String> mhas) throws SQLException {
        // test connection & authority 3 account
        int successCount = 0;
        StringBuilder msg = new StringBuilder();
        Map<String,Future<Pair<Boolean, String>>> futureMap = Maps.newHashMap();
        for (String mha : mhas) {
            Future<Pair<Boolean, String>> future = accountExecutorService.submit(() -> accountV2Check(mha));
            futureMap.put(mha,future);
        }
        for (Map.Entry<String,Future<Pair<Boolean, String>>> entry : futureMap.entrySet()) {
            try {
                Pair<Boolean, String> res = entry.getValue().get(30,TimeUnit.SECONDS);
                if (res.getKey()) {
                    successCount++;
                } else {
                    msg.append(entry.getKey()).append(" fail").append(res.getValue()).append("\n");
                }
            } catch (Exception e) {
                logger.error("accountV2Check error",e);
                msg.append(entry.getKey()).append(" fail").append(e.getMessage()).append("\n");
            }
        }
        return Pair.of(successCount, msg.toString());
    }
    
    
    
    private boolean initMhaPasswordToken(String mha) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mha);
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(mha + " not exist");
        }
        if (!StringUtils.isBlank(mhaTblV2.getMonitorPasswordToken()) 
                && !StringUtils.isBlank(mhaTblV2.getReadPasswordToken())
                && !StringUtils.isBlank(mhaTblV2.getWritePasswordToken())) {
            logger.info("mha:{} already init password token",mha);
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
    
    
    private Pair<Boolean,String> accountV2Check(String mha) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mha,0);
            if (mhaTblV2 == null) {
                logger.error("mha:{} not exist",mha);
                return Pair.of(false,"mha not exist");
            }
            MhaAccounts oldAccounts = getMhaAccounts(mhaTblV2, false);
            MhaAccounts newAccounts = getMhaAccounts(mhaTblV2, true);
            AccountPrivilegeCheckVo checkVo = new AccountPrivilegeCheckVo();
            checkVo.setAccounts(Lists.newArrayList());
            checkVo.setMhaName(mha);
            boolean res1 = this.privilegeCheck(mha, oldAccounts.getMonitorAcc(), newAccounts.getMonitorAcc(), checkVo);
            boolean res2 = this.privilegeCheck(mha, oldAccounts.getReadAcc(), newAccounts.getReadAcc(), checkVo);
            boolean res3 = this.privilegeCheck(mha, oldAccounts.getWriteAcc(), newAccounts.getWriteAcc(), checkVo);
            
            String errorTips = CollectionUtils.isEmpty(checkVo.getAccounts()) ? null : JsonUtils.toJson(checkVo);
            return Pair.of(res1&res2&res3, errorTips);
        } catch (Throwable e) {
            logger.error("mha:{} account check error",mha,e);
            return Pair.of(false, "account check error" + e.getMessage());
        }
    }
    
    private boolean privilegeCheck(String mha, Account oldAccount, Account newAccount, AccountPrivilegeCheckVo checkVo) {
        String oldAcc = oldAccount.getUser();
        String newAcc = newAccount.getUser();
        String oldPrivilegesString = mysqlServiceV2.queryAccountPrivileges(mha, oldAccount.getUser(), oldAccount.getPassword());
        String newPrivilegesString = mysqlServiceV2.queryAccountPrivileges(mha, newAccount.getUser(), newAccount.getPassword());
        if (oldPrivilegesString != null && newPrivilegesString != null) {
            String[] oldPrivileges = oldPrivilegesString.split(";");
            String[] newPrivileges = newPrivilegesString.split(";");
            if (oldPrivileges.length == newPrivileges.length) {
                for (int i = 0; i < oldPrivileges.length; i++) { // remove 'user'@'%' and compare
                    String oldPrivilege = oldPrivileges[i].substring(0,oldPrivileges[i].length()-oldAcc.length()-6);
                    String newPrivilege = newPrivileges[i].substring(0,newPrivileges[i].length()-newAcc.length()-6);
                    if (!oldPrivilege.equalsIgnoreCase(newPrivilege)) {
                        break;
                    } else {
                        if (i == oldPrivileges.length -1) { // all privilege equal
                            return true;
                        }
                    }
                }
            }
        }
        checkVo.getAccounts().add(new AccountPrivilege(oldAcc,StringUtils.isEmpty(oldPrivilegesString) ? "error" : oldPrivilegesString));
        checkVo.getAccounts().add(new AccountPrivilege(newAcc,StringUtils.isEmpty(newPrivilegesString) ? "error" : newPrivilegesString));
        return false;
    }
    
    
    private MhaAccounts getMhaAccounts(MhaTblV2 mhaTblV2,boolean newAccount) {
        if (newAccount) {
            return new MhaAccounts(
                    mhaTblV2.getMhaName(),
                    new Account(mhaTblV2.getMonitorUserV2(),this.decrypt(mhaTblV2.getMonitorPasswordTokenV2())),
                    new Account(mhaTblV2.getReadUserV2(),this.decrypt(mhaTblV2.getReadPasswordTokenV2())),
                    new Account(mhaTblV2.getWriteUserV2(),this.decrypt(mhaTblV2.getWritePasswordTokenV2()))
            );
        } else {
            return new MhaAccounts(
                    mhaTblV2.getMhaName(),
                    new Account(mhaTblV2.getMonitorUser(),this.decrypt(mhaTblV2.getMonitorPasswordToken())),
                    new Account(mhaTblV2.getReadUser(),this.decrypt(mhaTblV2.getReadPasswordToken())),
                    new Account(mhaTblV2.getWriteUser(),this.decrypt(mhaTblV2.getWritePasswordToken()))
            );
        }
    } 

    private boolean mhaAccountV2ChangePwd(String mha,boolean forceChange) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mha, 0);
        if(mhaTblV2 == null){
            throw ConsoleExceptionUtils.message(mha + " not exist");
        }
        if (!forceChange 
                && !StringUtils.isBlank(mhaTblV2.getMonitorUserV2())
                && !StringUtils.isBlank(mhaTblV2.getMonitorPasswordTokenV2())
                && !StringUtils.isBlank(mhaTblV2.getReadUserV2())
                && !StringUtils.isBlank(mhaTblV2.getReadPasswordTokenV2())
                && !StringUtils.isBlank(mhaTblV2.getWriteUserV2())
                && !StringUtils.isBlank(mhaTblV2.getWritePasswordTokenV2())) {
            logger.info("mha:{} already init account v2",mha);
            return true;
        }
        MhaAccounts mhaAccounts = dbaApiService.accountV2PwdChange(mhaTblV2);
        if (mhaAccounts == null) {
            throw ConsoleExceptionUtils.message("dbaApiService.initAccountV2 error");
        }
        mhaTblV2.setMonitorUserV2(mhaAccounts.getMonitorAcc().getUser());
        mhaTblV2.setMonitorPasswordTokenV2(this.encrypt(mhaAccounts.getMonitorAcc().getPassword()));
        mhaTblV2.setReadUserV2(mhaAccounts.getReadAcc().getUser());
        mhaTblV2.setReadPasswordTokenV2(this.encrypt(mhaAccounts.getReadAcc().getPassword()));
        mhaTblV2.setWriteUserV2(mhaAccounts.getWriteAcc().getUser());
        mhaTblV2.setWritePasswordTokenV2(this.encrypt(mhaAccounts.getWriteAcc().getPassword()));
        return mhaTblV2Dao.update(mhaTblV2) == 1;
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
