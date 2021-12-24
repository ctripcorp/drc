package com.ctrip.framework.drc.core.server.utils;

import org.apache.commons.lang3.StringUtils;

import java.net.*;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/16 下午11:18.
 */
public class IpUtils {

    public static String getFistNonLocalIpv4ServerAddress() {
        String first = getProperty("host.ip");
        if (StringUtils.isNotBlank(first)) {
            return first;
        }

        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces == null) {
                return null;
            }
            while (interfaces.hasMoreElements()) {
                NetworkInterface current = interfaces.nextElement();
                if (current.isLoopback()) {
                    continue;
                }
                List<InterfaceAddress> addresses = current.getInterfaceAddresses();
                if (addresses.size() == 0) {
                    continue;
                }
                for (InterfaceAddress interfaceAddress : addresses) {
                    InetAddress address = interfaceAddress.getAddress();
                    if (address instanceof Inet4Address && address.isSiteLocalAddress() && !current.getName().startsWith("veth")) {
                        if(first == null){
                            first = address.getHostAddress();
                        }
                    }
                }
            }
        } catch (SocketException e) {
        }

        if(first != null){
            return first;
        }
        throw new IllegalStateException("[can not find a qualified address]");
    }

    private static String getProperty(String name) {
        String value = System.getProperty(name);

        if (value == null) {
            value = System.getenv(name);
        }

        return value;
    }
}
