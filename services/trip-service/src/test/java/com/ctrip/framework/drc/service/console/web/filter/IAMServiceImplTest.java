package com.ctrip.framework.drc.service.console.web.filter;

import com.ctrip.basebiz.offline.iam.apifacade.contract.IAMFacadeServiceClient;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.ResultItem;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeRequestType;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeResponseType;
import com.ctrip.framework.drc.service.config.QConfig;
import com.ctriposs.baiji.rpc.common.types.AckCodeType;
import com.ctriposs.baiji.rpc.common.types.ResponseStatusType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class IAMServiceImplTest {
    
    @InjectMocks
    private IAMServiceImpl iAMServiceImpl;
    
    @Mock
    private QConfig qConfig;
    
    @Mock
    private IAMFacadeServiceClient iamSoaService;
    
    @Before
    public void setUp() {
        System.setProperty("iam.config.enable","off");
        MockitoAnnotations.openMocks(this);
        Mockito.when(qConfig.get(Mockito.eq("iam.filter.switch"), Mockito.any())).thenReturn("on");
        Mockito.when(qConfig.get(Mockito.eq("query.all.db.permission.code"), Mockito.any())).thenReturn("code3");
        Mockito.when(qConfig.get(Mockito.eq("create.mq.meta.permission.code"), Mockito.any())).thenReturn("code3");
    }
    
    @Test
    public void testMatchApiPermissionCode() {
        Mockito.when(qConfig.get(Mockito.eq("/api1/*"), Mockito.any())).thenReturn("code1");
        Mockito.when(qConfig.get(Mockito.eq("/api2/*"), Mockito.any())).thenReturn("code2");
        Mockito.when(qConfig.getKeys()).thenReturn(Sets.newHashSet("/api1/*"));
        iAMServiceImpl.onChange("/api1/*", null,null);
        Assert.assertEquals("code1", iAMServiceImpl.matchApiPermissionCode("http://www.test.com/api1/1"));
        Assert.assertNull(iAMServiceImpl.matchApiPermissionCode("http://www.test.com/api2/2"));
        Mockito.when(qConfig.getKeys()).thenReturn(Sets.newHashSet("/api1/*","/api2/*"));
        iAMServiceImpl.onChange("/api2/*", null,null);
        Assert.assertEquals("code2", iAMServiceImpl.matchApiPermissionCode("http://www.test.com/api2/2"));
        
    }
    
    
    @Test
    public void testIamFilterEnable() {
        assertTrue(iAMServiceImpl.iamFilterEnable());
    }
    

    @Test
    public void testCheckPermission() throws Exception {
        
        // case1 ack fail
        VerifyByBatchCodeResponseType res = new VerifyByBatchCodeResponseType();
        ResponseStatusType responseStatusType = new ResponseStatusType();
        responseStatusType.setAck(AckCodeType.Failure);
        res.setResponseStatus(responseStatusType);
        Mockito.when(iamSoaService.verifyByBatchCode(Mockito.any(VerifyByBatchCodeRequestType.class))).thenReturn(res);
        Assert.assertFalse(iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getLeft());
        Assert.assertEquals("verifyByBatchCode error",iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getRight());
        
        // case2 permission code check res is forbidden
        responseStatusType = new ResponseStatusType();
        responseStatusType.setAck(AckCodeType.Success);
        res.setResponseStatus(responseStatusType);
        res.setResults(new HashMap<>(){{put("code1", new ResultItem(false, "code1", null));}});

        Assert.assertFalse(iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getLeft());
        Assert.assertEquals("code1",iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getRight());


        // case3 permission code res true
        responseStatusType = new ResponseStatusType();
        responseStatusType.setAck(AckCodeType.Success);
        res.setResponseStatus(responseStatusType);
        res.setResults(new HashMap<>(){{put("code1", new ResultItem(true, "code1", null));}});

        Assert.assertTrue(iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getLeft());
        Assert.assertNull(iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getRight());

        // test checkQueryAllCflLogPermission
        Assert.assertTrue(iAMServiceImpl.canQueryAllDbReplication().getLeft());
        Assert.assertNull(iAMServiceImpl.canQueryAllDbReplication().getRight());


        // case4 check exception
        Mockito.when(iamSoaService.verifyByBatchCode(Mockito.any(VerifyByBatchCodeRequestType.class))).thenThrow(new RuntimeException("test"));
        Assert.assertFalse(iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getLeft());
        Assert.assertEquals("verifyByBatchCode error",iAMServiceImpl.checkPermission(Lists.newArrayList("code1"),"eid").getRight());

    }
}