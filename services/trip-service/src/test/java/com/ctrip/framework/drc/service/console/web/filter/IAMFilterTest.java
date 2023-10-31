package com.ctrip.framework.drc.service.console.web.filter;

import static org.junit.Assert.*;

import com.ctrip.basebiz.offline.iam.apifacade.contract.IAMFacadeServiceClient;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.ResultItem;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeRequestType;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeResponseType;
import com.ctrip.infosec.sso.client.CtripSSOTools;
import com.ctriposs.baiji.rpc.common.types.AckCodeType;
import com.ctriposs.baiji.rpc.common.types.ResponseStatusType;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class IAMFilterTest {
    
    @InjectMocks
    private IAMFilter iamFilter;
    
    @Mock
    private IAMFilterService iamFilterService;
    
    @Mock
    private IAMFacadeServiceClient iamSoaService;

    @Before
    public void setUp() throws Exception {
        System.setProperty("iam.config.enable","off");
        MockitoAnnotations.openMocks(this);
        Mockito.when(iamFilterService.iamFilterEnable()).thenReturn(true);
    }

    @Test
    public void testDoFilter() throws Exception {
        // mock HttpServletRequest & HttpServletResponse
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        FilterChain filterChain = Mockito.mock(FilterChain.class);
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:8080/api1/1"));

        // mock the returned value of request.getParameterMap()
        Mockito.doNothing().when(filterChain).doFilter(Mockito.any(), Mockito.any());
        Mockito.when(response.getWriter()).thenReturn(new PrintWriter(new OutputStreamWriter(System.out)));

        // case1 no eid
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn(null);
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1)).doFilter(Mockito.any(), Mockito.any());
        }
        
        // case2 no need check permission
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");
            Mockito.when(iamFilterService.getApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn(null);
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1)).doFilter(Mockito.any(), Mockito.any());
        }

        // case3 ack fail
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");
            VerifyByBatchCodeResponseType res = new VerifyByBatchCodeResponseType();
            ResponseStatusType responseStatusType = new ResponseStatusType();
            responseStatusType.setAck(AckCodeType.Failure);
            res.setResponseStatus(responseStatusType);
            Mockito.when(iamFilterService.getApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn("code1");
            Mockito.when(iamSoaService.verifyByBatchCode(Mockito.any(VerifyByBatchCodeRequestType.class))).thenReturn(res);
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1+0)).doFilter(Mockito.any(), Mockito.any());
        }


        // case4 permission code check res is forbidden
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");
            
            VerifyByBatchCodeResponseType res = new VerifyByBatchCodeResponseType();
            ResponseStatusType responseStatusType = new ResponseStatusType();
            responseStatusType.setAck(AckCodeType.Success);
            res.setResponseStatus(responseStatusType);
            res.setResults(new HashMap<>(){{put("code1", new ResultItem(false, "code1", null));}});
            
            Mockito.when(iamFilterService.getApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn("code1");
            Mockito.when(iamSoaService.verifyByBatchCode(Mockito.any(VerifyByBatchCodeRequestType.class))).thenReturn(res);
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1+0+0)).doFilter(Mockito.any(), Mockito.any());
        }

        // case5 permission code res true
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");

            VerifyByBatchCodeResponseType res = new VerifyByBatchCodeResponseType();
            ResponseStatusType responseStatusType = new ResponseStatusType();
            responseStatusType.setAck(AckCodeType.Success);
            res.setResponseStatus(responseStatusType);
            res.setResults(new HashMap<>(){{put("code1", new ResultItem(true, "code1", null));}});

            Mockito.when(iamFilterService.getApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn("code1");
            Mockito.when(iamSoaService.verifyByBatchCode(Mockito.any(VerifyByBatchCodeRequestType.class))).thenReturn(res);
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1+0+0+1)).doFilter(Mockito.any(), Mockito.any());
        }

        // case6 permission code check res is forbidden
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");
            Mockito.when(iamFilterService.getApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn("code1");
            Mockito.when(iamSoaService.verifyByBatchCode(Mockito.any(VerifyByBatchCodeRequestType.class))).thenThrow(new RuntimeException("error"));
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1+0+0+1+0)).doFilter(Mockito.any(), Mockito.any());
        }
        
        
    }
    
}