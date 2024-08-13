package com.ctrip.framework.drc.console.service.v2.security.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.DrcTmpconninfo;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.security.DataSourceCrypto;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.vo.check.v2.AccountPrivilege;
import com.ctrip.framework.drc.console.vo.check.v2.AccountPrivilegeCheckVo;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

public class AccountServiceImplTest {
    
    @InjectMocks
    AccountServiceImpl accountServiceImpl;

    @Spy
    private DataSourceCrypto dataSourceCrypto = new DataSourceCryptoImpl();
    
    @Mock
    private KmsService kmsService;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DbaApiService dbaApiService;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    
    
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(consoleConfig.isCenterRegion()).thenReturn(true);
        when(consoleConfig.getKMSAccessToken("account")).thenReturn("kmsAccountAccessToken");
        when(consoleConfig.getAccountKmsTokenSwitch()).thenReturn(true);
        when(consoleConfig.getAccountKmsTokenMhaGray()).thenReturn(Sets.newHashSet(Lists.newArrayList("mha1","mha2")));
        when(consoleConfig.getAccountKmsTokenSwitchV2()).thenReturn(true);
        when(consoleConfig.getAccountKmsTokenMhaGrayV2()).thenReturn(Sets.newHashSet(Lists.newArrayList("mha1")));
        when(kmsService.getSecretKey(Mockito.eq("kmsAccountAccessToken"))).thenReturn("o$:K@0ktUc0<7mkO");
    }
    
    

    @Test
    public void testInitAndDecrypt() throws Exception {
        System.setProperty("mock.data.path.prefix","/testData/accountService/");
        when(mhaTblV2Dao.queryAllExist()).thenReturn(getData().stream().filter(mhaTblV2 -> mhaTblV2.getMhaName().equals("mha1")).collect(Collectors.toList()));
        accountServiceImpl.init();
        Assert.assertEquals(1,accountServiceImpl.decryptCache.size());
        Assert.assertTrue(accountServiceImpl.decryptCache.values().contains("root1"));
    }
    
    private List<MhaTblV2> getData()throws Exception{
        String json = IOUtils.toString(Objects.requireNonNull(this.getClass().getResourceAsStream("/testData/accountService/MhaTblV2.json")), StandardCharsets.UTF_8);
        return JSON.parseArray(json, MhaTblV2.class);
    }
    
    @Test
    public void testEncrypt() {
        String encrypt = accountServiceImpl.encrypt("root2");
        accountServiceImpl.encrypt("root2");
        Assert.assertEquals("root2",accountServiceImpl.decrypt(encrypt));
    }

    @Test
    public void testGetMhaAccounts() throws Exception {
        List<MhaTblV2> data = this.getData().stream().filter(mhaTblV2 -> mhaTblV2.getMhaName().equals("mha1")).collect(Collectors.toList());
        when(mhaTblV2Dao.queryAllExist()).thenReturn(data);
        for (MhaTblV2 mhaTblV2 : data) {
            MhaAccounts mhaAccounts = accountServiceImpl.getMhaAccounts(mhaTblV2);
            Assert.assertNotNull(mhaAccounts.getMonitorAcc().getUser());
            Assert.assertNotNull(mhaAccounts.getMonitorAcc().getPassword());
            Assert.assertNotNull(mhaAccounts.getReadAcc().getUser());
            Assert.assertNotNull(mhaAccounts.getReadAcc().getPassword());
            Assert.assertNotNull(mhaAccounts.getWriteAcc().getUser());
            Assert.assertNotNull(mhaAccounts.getWriteAcc().getPassword());
            if (mhaTblV2.getMhaName().equalsIgnoreCase("mha1")) {
                Assert.assertEquals("root1", mhaAccounts.getMonitorAcc().getUser());
                Assert.assertEquals("root1", mhaAccounts.getMonitorAcc().getPassword());
            }
        }
    }
    
    @Test
    public void testInitMhaPasswordToken() throws Exception {
        List<MhaTblV2> data = this.getData();
        when(mhaTblV2Dao.queryByMhaName(anyString())).thenAnswer(
            invocation -> {
                return data.stream().filter(mhaTblV2 -> mhaTblV2.getMhaName().equals(invocation.getArgument(0))).findFirst().orElse(null);
            }
        );
        when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        try {
            Pair<Boolean, Integer> res = accountServiceImpl.initMhaPasswordToken(
                    Lists.newArrayList("mha1", "mha2", "mha3"));
            Assert.assertTrue(res.getLeft());
            Assert.assertEquals(3, res.getRight().intValue());

            res = accountServiceImpl.initMhaPasswordToken(
                    Lists.newArrayList("mha1", "mha2", "mha4"));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConsoleException);
            Assert.assertEquals("mha4 not exist", e.getMessage());
        }
        
    }
    
    
    @Test
    public void testDoEncryptAndDecrypt() {
        String content = "root";
        String secretKey = "o$:K@0ktUc0<7mkO"; // test secretKey ,no use
        String encrypt = dataSourceCrypto.encrypt(content, secretKey);
        String decrypt = dataSourceCrypto.decrypt(encrypt, secretKey);
        assertEquals(content, decrypt);
        String encrypt1 = dataSourceCrypto.encrypt("root1", secretKey);
        String decrypt1 = dataSourceCrypto.decrypt(encrypt1, secretKey);
        assertEquals("root1", decrypt1);
    }

    @Test
    public void testInitMhaAccountV2() throws Exception {
        List<MhaTblV2> data = this.getData();
        when(mhaTblV2Dao.queryByMhaName(anyString(),Mockito.eq(0))).thenAnswer(
                invocation -> data.stream().filter(mhaTblV2 -> mhaTblV2.getMhaName().equals(invocation.getArgument(0)))
                        .findFirst().orElse(null)
        );
        MhaAccounts mhaAccounts = accountServiceImpl.getMhaAccounts(data.get(0));// mha1 account, root1
        when(dbaApiService.accountV2PwdChange(Mockito.any(MhaTblV2.class))).thenReturn(mhaAccounts);
        when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        
        Pair<Integer, String> res = accountServiceImpl.mhaAccountV2ChangePwd(
                Lists.newArrayList("mha1", "mha2", "mha3"), false);
        Assert.assertEquals(3, res.getLeft().intValue());
    }

    @Test
    public void testAccountV2Check() throws Exception {
        List<MhaTblV2> data = this.getData();
        when(mhaTblV2Dao.queryByMhaName(anyString(),Mockito.eq(0))).thenAnswer(
                invocation -> data.stream().filter(mhaTblV2 -> mhaTblV2.getMhaName().equals(invocation.getArgument(0)))
                        .findFirst().orElse(null)
        );
        when(mysqlServiceV2.queryAccountPrivileges(anyString(),anyString(),anyString())).thenAnswer(
                invocation -> {
                    String user = invocation.getArgument(1);
                    String privilegeFormatter = "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `%s`@`%%`;GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `%s`@`%%`";
                    return String.format(privilegeFormatter, user,user);
                }
        );
        
        Pair<Integer, String> res = accountServiceImpl.accountV2Check(Lists.newArrayList("mha1"));
        Assert.assertEquals(1, res.getLeft().intValue());

        DrcTmpconninfo drcTmpconninfo = new DrcTmpconninfo();
        drcTmpconninfo.setId(1L);
        drcTmpconninfo.setHost("ip");
        drcTmpconninfo.setPort(3306);
        drcTmpconninfo.setDbUser("root");
        drcTmpconninfo.setPassword(new Byte[]{1,2,3,4});
        drcTmpconninfo.setDatachangeCreateTime(new Timestamp(System.currentTimeMillis()));
        drcTmpconninfo.setDatachangeLasttime(new Timestamp(System.currentTimeMillis())); 

        drcTmpconninfo.getId();
        drcTmpconninfo.getHost();
        drcTmpconninfo.getPort();
        drcTmpconninfo.getDbUser();
        drcTmpconninfo.getPassword();
        drcTmpconninfo.getDatachangeCreateTime();
        drcTmpconninfo.getDatachangeLasttime();
    }
    
//    @Test
//    public void parseCheckRes() {
//        String[] split = s.split("\\\\n");
//        StringBuilder standardJson = new StringBuilder("[");
//        for (int i = 0; i < split.length; i++) {
//            String mhaCheckRes = split[i];
//            String s1 = mhaCheckRes.replaceAll("\\\\", "");
//            int indexOf = s1.indexOf("{");
//            s1 = s1.substring(indexOf);
//            if (i != 0) {
//                standardJson.append(",");
//            }
//            standardJson.append(s1);
//        }
//        standardJson.append("]");
//        System.out.println(standardJson.toString());
//        List<AccountPrivilegeCheckVo> checkRes = JsonUtils.fromJsonToList(standardJson.toString(),
//                AccountPrivilegeCheckVo.class);
//        System.out.println("diffCount" + checkRes.size());
//
//        // sout fail ,change pwd , todo ,check"="
//        List<AccountPrivilegeCheckVo> accountConnectFail = checkRes.stream().filter(
//                vo -> vo.getAccounts().stream().anyMatch(
//                        account -> account.getRes().contains("error")
//                )
//        ).collect(Collectors.toList());
//        StringBuilder sb = new StringBuilder("[");
//        for (int i = 0; i < accountConnectFail.size(); i++) {
//            if(i != 0) {
//                sb.append(",");
//            }
//            sb.append("\"").append(accountConnectFail.get(i).getMhaName()).append("\"");
//        }
//        sb.append("]");
//        System.out.println("accountConnectFail: " + sb.toString());
//        System.out.println("diffCount" + checkRes.size());
//        
//        // find AccountV2NotStandard
//        for (int i = 0; i < checkRes.size(); i++) {
//            AccountPrivilegeCheckVo checkVo = checkRes.get(i);
//            for (AccountPrivilege account : checkVo.getAccounts()) {
//                String acc = account.getAcc();
//                if (acc.equalsIgnoreCase("m_drcconsolev1")) {
//                    boolean standard = account.getRes().equalsIgnoreCase("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'm_drcconsolev1'@'%';GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON `drcmonitordb`.* TO 'm_drcconsolev1'@'%'");
//                    standard |= account.getRes().equalsIgnoreCase("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `m_drcconsolev1`@`%`;GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON `drcmonitordb`.* TO `m_drcconsolev1`@`%`");
//                    if (!standard) {
//                        System.out.println("m_drcconsolev1 NotStandard: " + account.toString());
//                    }
//                } else if (acc.equalsIgnoreCase("m_drcv1_r")) {
//                    boolean standard = account.getRes().equalsIgnoreCase("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `m_drcv1_r`@`%`");
//                    standard |= account.getRes().equalsIgnoreCase("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'm_drcv1_r'@'%'");
//                    if (!standard) {
//                        System.out.println("m_drcv1_r NotStandard: " + account.toString());
//                    }
//                } else if (acc.equalsIgnoreCase("m_drcv1_w")) {
//                    boolean standard = account.getRes().equalsIgnoreCase("GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO 'm_drcv1_w'@'%'");
//                    standard |= account.getRes().equalsIgnoreCase("GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO `m_drcv1_w`@`%`");
//                    if (!standard) {
//                        System.out.println("m_drcv1_w NotStandard: " + account.toString());
//                    }
//                }
//            }
//        }
//        
//
//        // find AccountPrivilegeCheckVo accountList contains "m_drcconsole" and "m_drc_r" and "m_drc_w"
////        List<AccountPrivilegeCheckVo> affConsole = checkRes.stream().filter(
////                vo -> vo.getAccounts().stream().anyMatch(
////                        account -> account.getAcc().contains("m_drcconsole")
////                )
////        ).collect(Collectors.toList());
////        System.out.println("diffConsole:" + affConsole.size());
////
////        List<AccountPrivilegeCheckVo> affR = checkRes.stream().filter(
////                vo -> vo.getAccounts().stream().anyMatch(
////                        account -> account.getAcc().contains("m_drc_r")
////                )
////        ).collect(Collectors.toList());
////        System.out.println("diffR:" + affR.size());
////
////        List<AccountPrivilegeCheckVo> affW = checkRes.stream().filter(
////                vo -> vo.getAccounts().stream().anyMatch(
////                        account -> account.getAcc().contains("m_drc_w")
////                )
////        ).collect(Collectors.toList());
////        System.out.println("diffW:" + affW.size());
//    }


    
}