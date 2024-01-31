package com.ctrip.framework.drc.console.param.log;

/**
 * @ClassName ConflictDbBlacklistDto
 * @Author haodongPan
 * @Date 2024/1/31 14:49
 * @Version: $
 */
public class ConflictDbBlacklistDto {
    private Long id;
    private String dbFilter;
    private Long expirationTime;
    private Integer type;

    @Override
    public String toString() {
        return "ConflictDbBlacklistDto{" +
                "id=" + id +
                ", dbFilter='" + dbFilter + '\'' +
                ", expirationTime=" + expirationTime +
                ", type=" + type +
                '}';
    }

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

    public Long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(Long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }
    
}
