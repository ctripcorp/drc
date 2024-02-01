package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2024/1/15 14:42
 */
public class ConflictDbBlacklistView {
    private Long id;
    private String dbFilter;
    private Integer type;
    private String createTime;
    private String expirationTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDbFilter() {
        return dbFilter;
    }

    public void setDbFilter(String dbFilter) {
        this.dbFilter = dbFilter;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(String expirationTime) {
        this.expirationTime = expirationTime;
    }
}
