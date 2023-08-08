package com.ctrip.framework.drc.console.dto;

public class BuildMhaDto {

    private String buName;

    private String dalClusterName;

    private Long appid;

    private String originalMha;

    private String originalMhaDc;

    private String newBuiltMha;

    private String newBuiltMhaDc;

    private String originTag;

    private String dstTag;

    public String getOriginTag() {
        return originTag;
    }

    public void setOriginTag(String originTag) {
        this.originTag = originTag;
    }

    public String getDstTag() {
        return dstTag;
    }

    public void setDstTag(String dstTag) {
        this.dstTag = dstTag;
    }

    public String getBuName() {
        return buName;
    }

    public BuildMhaDto setBuName(String buName) {
        this.buName = buName;
        return this;
    }

    public String getDalClusterName() {
        return dalClusterName;
    }

    public BuildMhaDto setDalClusterName(String dalClusterName) {
        this.dalClusterName = dalClusterName;
        return this;
    }

    public Long getAppid() {
        return appid;
    }

    public BuildMhaDto setAppid(Long appid) {
        this.appid = appid;
        return this;
    }

    public String getOriginalMha() {
        return originalMha;
    }

    public BuildMhaDto setOriginalMha(String originalMha) {
        this.originalMha = originalMha;
        return this;
    }

    public String getNewBuiltMha() {
        return newBuiltMha;
    }

    public BuildMhaDto setNewBuiltMha(String newBuiltMha) {
        this.newBuiltMha = newBuiltMha;
        return this;
    }

    public String getNewBuiltMhaDc() {
        return newBuiltMhaDc;
    }

    public BuildMhaDto setNewBuiltMhaDc(String newBuiltMhaDc) {
        this.newBuiltMhaDc = newBuiltMhaDc;
        return this;
    }

    public String getOriginalMhaDc() {
        return originalMhaDc;
    }

    public BuildMhaDto setOriginalMhaDc(String originalMhaDc) {
        this.originalMhaDc = originalMhaDc;
        return this;
    }

    @Override
    public String toString() {
        return "BuildMhaDto{" +
                "buName='" + buName + '\'' +
                ", dalClusterName='" + dalClusterName + '\'' +
                ", appid=" + appid +
                ", originalMha='" + originalMha + '\'' +
                ", originalMhaDc='" + originalMhaDc + '\'' +
                ", newBuiltMha='" + newBuiltMha + '\'' +
                ", newBuiltMhaDc='" + newBuiltMhaDc + '\'' +
                '}';
    }
}
