package com.ctrip.framework.drc.core.service.statistics.traffic;

/**
 * Created by jixinwang on 2022/9/11
 */
public class HickWallTrafficContext {

    private String srcRegion;

    private String dstRegion;

    private Long endTime;

    private String baseUrl;

    private String accessToken;

    public HickWallTrafficContext(String srcRegion, String dstRegion, Long endTime, String baseUrl, String accessToken) {
        this.srcRegion = srcRegion;
        this.dstRegion = dstRegion;
        this.endTime = endTime;
        this.baseUrl = baseUrl;
        this.accessToken = accessToken;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
}
