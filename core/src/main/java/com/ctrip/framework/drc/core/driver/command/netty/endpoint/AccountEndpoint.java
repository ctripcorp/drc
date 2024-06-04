package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import java.util.Objects;

/**
 * @ClassName AccValidateEndPoint
 * @Author haodongPan
 * @Date 2024/6/4 11:02
 * @Version: $
 * Distinguish based on account
 */
public class AccountEndpoint extends MySqlEndpoint {

    public AccountEndpoint(String ip, int port, String username, String password, boolean master) {
        super(ip, port, username, password, master);
    }

    @Override
    public String toString() {
        return super.toString() + ", master: " + isMaster();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSocketAddress().toString() + getUser() + getPassword() + isMaster());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AccountEndpoint) {
            AccountEndpoint other = (AccountEndpoint) obj;
            return Objects.equals(getSocketAddress(), other.getSocketAddress()) &&
                    Objects.equals(getUser(), other.getUser()) &&
                    Objects.equals(getPassword(), other.getPassword()) &&
                    isMaster() == other.isMaster();
        }
        return false;
    }
}