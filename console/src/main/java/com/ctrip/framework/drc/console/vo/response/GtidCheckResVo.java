package com.ctrip.framework.drc.console.vo.response;

/**
 * @ClassName GtidCheckResVo
 * @Author haodongPan
 * @Date 2023/6/5 11:17
 * @Version: $
 */
public class GtidCheckResVo {
    
    private boolean legal;
    private String purgedGtid;

    public GtidCheckResVo() {
    }

    public GtidCheckResVo(boolean legal, String purgedGtid) {
        this.legal = legal;
        this.purgedGtid = purgedGtid;
    }

    public boolean getLegal() {
        return legal;
    }

    public void setLegal(boolean legal) {
        this.legal = legal;
    }

    public String getPurgedGtid() {
        return purgedGtid;
    }

    public void setPurgedGtid(String purgedGtid) {
        this.purgedGtid = purgedGtid;
    }
}
