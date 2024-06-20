package com.ctrip.framework.drc.console.service.v2.security.impl;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.impl.CommonDataInit;
import com.ctrip.framework.drc.console.service.v2.security.DataSourceCrypto;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
import org.springframework.beans.factory.annotation.Autowired;

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
        when(mhaTblV2Dao.queryAllExist()).thenReturn(getData());
        accountServiceImpl.init();
        Assert.assertEquals(1,accountServiceImpl.decryptCache.size());
        Assert.assertTrue(accountServiceImpl.decryptCache.values().contains("root"));
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
        List<MhaTblV2> data = this.getData();
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
        when(mhaTblV2Dao.queryByMhaName(Mockito.anyString())).thenAnswer(
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
        when(mhaTblV2Dao.queryByMhaName(Mockito.anyString(),Mockito.eq(0))).thenAnswer(
                invocation -> data.stream().filter(mhaTblV2 -> mhaTblV2.getMhaName().equals(invocation.getArgument(0)))
                        .findFirst().orElse(null)
        );
        MhaAccounts mhaAccounts = accountServiceImpl.getMhaAccounts(data.get(0));// mha1 account, root1
        when(dbaApiService.initAccountV2(Mockito.any(MhaTblV2.class))).thenReturn(mhaAccounts);
        when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        
        Pair<Boolean, Integer> res = accountServiceImpl.initMhaAccountV2(
                Lists.newArrayList("mha1", "mha2", "mha3"));
        Assert.assertTrue(res.getLeft());
        Assert.assertEquals(3, res.getRight().intValue());
    }
}