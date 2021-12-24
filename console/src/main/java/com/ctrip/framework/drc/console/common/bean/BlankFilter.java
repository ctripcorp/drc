package com.ctrip.framework.drc.console.common.bean;

import javax.servlet.*;
import javax.servlet.FilterConfig;
import java.io.IOException;

/**
 * @ClassName BlankFilter
 * @Author haodongPan
 * @Date 2021/12/6 17:09
 * @Version: $
 */
public class BlankFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

    }

    @Override
    public void destroy() {

    }
}
