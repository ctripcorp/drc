package com.ctrip.framework.drc.console.vo.api;

/**
 * @ClassName MessengerInfo
 * @Author haodongPan
 * @Date 2023/1/10 17:44
 * @Version: $
 */
public class MessengerInfo {
    
    private String mhaName;
    
    private String nameFilter;

    @Override
    public String toString() {
        return "MessengerInfo{" +
                "mhaName='" + mhaName + '\'' +
                ", nameFilter='" + nameFilter + '\'' +
                '}';
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }
}
