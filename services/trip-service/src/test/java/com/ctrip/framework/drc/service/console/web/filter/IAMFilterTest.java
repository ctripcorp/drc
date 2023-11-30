package com.ctrip.framework.drc.service.console.web.filter;

import com.ctrip.infosec.sso.client.CtripSSOTools;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.util.Lists;
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
    private IAMServiceImpl iamServiceImpl;

    @Before
    public void setUp() throws Exception {
        System.setProperty("iam.config.enable","off"); // skip the constructor of IAMServiceImpl
        MockitoAnnotations.openMocks(this);
        Mockito.when(iamServiceImpl.iamFilterEnable()).thenReturn(true);
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
            Mockito.when(iamServiceImpl.matchApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn(null);
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1)).doFilter(Mockito.any(), Mockito.any());
        }

        // case3 ack fail / no permission
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");
            Mockito.when(iamServiceImpl.matchApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn("code1");
            Mockito.when(iamServiceImpl.checkPermission(Mockito.eq(Lists.newArrayList("code1")),Mockito.eq("eid1"))).thenReturn(
                    Pair.of(false, "verifyByBatchCode error"));
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1+0)).doFilter(Mockito.any(), Mockito.any());
        }

        // case4 has permission
        try (MockedStatic<CtripSSOTools> mocked = Mockito.mockStatic(CtripSSOTools.class)) {
            mocked.when(CtripSSOTools::getEid).thenReturn("eid1");
            Mockito.when(iamServiceImpl.matchApiPermissionCode(Mockito.eq("http://localhost:8080/api1/1"))).thenReturn("code1");
            Mockito.when(iamServiceImpl.checkPermission(Mockito.eq(Lists.newArrayList("code1")),Mockito.eq("eid1"))).thenReturn(
                    Pair.of(true, null));
            iamFilter.doFilter(request, response, filterChain);
            Mockito.verify(filterChain, Mockito.times(1+1+0+1)).doFilter(Mockito.any(), Mockito.any());
        }
        
        
        
    }
    
}