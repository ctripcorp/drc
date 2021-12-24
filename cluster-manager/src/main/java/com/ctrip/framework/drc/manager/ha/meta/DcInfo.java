package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public class DcInfo {
    private String metaServerAddress;

    public DcInfo(){

    }

    public DcInfo(String metaServerAddress){
        this.metaServerAddress = metaServerAddress;
    }

    public String getMetaServerAddress() {
        return metaServerAddress;
    }

    public void setMetaServerAddress(String metaServerAddress) {
        this.metaServerAddress = metaServerAddress;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof DcInfo) {
            DcInfo other = (DcInfo) obj;
            if (!ObjectUtils.equals(metaServerAddress, other.metaServerAddress)) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {

        return ObjectUtils.hashCode(metaServerAddress);
    }

    @Override
    public String toString() {

        return String.format("metaServerAddress:%s", this.metaServerAddress);
    }
}
