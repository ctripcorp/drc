package ctrip.framework.drc.mysql;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class DbKey {

    private String name;

    private int port;

    public DbKey(String name, int port) {
        this.name = name;
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbKey)) return false;
        DbKey dbKey = (DbKey) o;
        return port == dbKey.port &&
                Objects.equals(name, dbKey.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, port);
    }

    @Override
    public String toString() {
        return "DbKey{" +
                "name='" + name + '\'' +
                ", port=" + port +
                '}';
    }
}
