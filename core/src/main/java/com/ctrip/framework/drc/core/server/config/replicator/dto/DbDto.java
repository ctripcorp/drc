package com.ctrip.framework.drc.core.server.config.replicator.dto;

import com.ctrip.framework.drc.core.entity.Db;

/**
 * @ClassName DbDto
 * @Author haodongPan
 * @Date 2024/8/6 18:46
 * @Version: $
 */
public class DbDto {
    private String ip;

    private Integer port;

    private boolean master;

    private String uuid;

    public static DbDto from(Db db) {
        DbDto dbDto = new DbDto();
        dbDto.setIp(db.getIp());
        dbDto.setPort(db.getPort());
        dbDto.setMaster(db.getMaster());
        dbDto.setUuid(db.getUuid());
        return dbDto;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}