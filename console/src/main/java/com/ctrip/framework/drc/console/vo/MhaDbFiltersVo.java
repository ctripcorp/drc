package com.ctrip.framework.drc.console.vo;

import java.util.List;

/**
 * @ClassName MhaDbFilterVo
 * @Author haodongPan
 * @Date 2022/1/11 17:09
 * @Version: $
 */
public class MhaDbFiltersVo {
    private String mhaName;
    
    private String dc;
    
    private String ipport;
    
    private List<String> filters;
    

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public String getIpport() {
        return ipport;
    }

    public void setIpport(String ipport) {
        this.ipport = ipport;
    }

    public List<String> getFilters() {
        return filters;
    }

    public void setFilters(List<String> filters) {
        this.filters = filters;
    }

    public MhaDbFiltersVo() {
    }

    public MhaDbFiltersVo(String mhaName, String dc, String ipport, List<String> filters) {
        this.mhaName = mhaName;
        this.dc = dc;
        this.ipport = ipport;
        this.filters = filters;
    }
}
