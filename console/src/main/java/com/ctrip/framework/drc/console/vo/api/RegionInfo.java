package com.ctrip.framework.drc.console.vo.api;

import java.util.Objects;

/**
 * @author yongnian
 * @create 2025/4/25 14:28
 */
public class RegionInfo {
    private final String src;
    private final String dst;

    public RegionInfo(String src, String dst) {
        this.src = src;
        this.dst = dst;
    }

    public String getSrc() {
        return src;
    }

    public String getDst() {
        return dst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RegionInfo that)) return false;
        return Objects.equals(src, that.src) && Objects.equals(dst, that.dst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(src, dst);
    }

    @Override
    public String toString() {
        return "RegionInfo{" +
                "src='" + src + '\'' +
                ", dst='" + dst + '\'' +
                '}';
    }
}
