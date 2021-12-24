package com.ctrip.framework.drc.core.meta;

import java.util.Objects;

/**
 * @Author Slight
 * Nov 07, 2019
 */
public class DBInfo extends InstanceInfo {
    public String username;
    public String password;
    public String uuid;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getURL() {
        return "jdbc:mysql://" + ip + ":" + port + "?allowMultiQueries=true&useLocalSessionState=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
    }

    @Override
    public String toString() {
        return "DBInfo{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", uuid='" + uuid + '\'' +
                ", name='" + name + '\'' +
                ", port=" + port +
                ", ip='" + ip + '\'' +
                ", idc='" + idc + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DBInfo)) return false;
        if (!super.equals(o)) return false;
        DBInfo dbInfo = (DBInfo) o;
        return Objects.equals(username, dbInfo.username) &&
                Objects.equals(password, dbInfo.password) &&
                Objects.equals(uuid, dbInfo.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), username, password, uuid);
    }
}
