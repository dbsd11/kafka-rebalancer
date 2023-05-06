package group.bison.kafka.rebalancer.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InetUtils {

    private static String hostAddress = "";

    public static String getLocalHostAddress() {
        if (StringUtils.isEmpty(hostAddress)) {
            hostAddress = "127.0.0.1";
            try {
                Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
                while (allNetInterfaces.hasMoreElements()) {
                    NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress ip = (InetAddress) addresses.nextElement();
                        if (ip != null
                                && ip instanceof Inet4Address
                                && !ip.isLoopbackAddress() // loopback地址即本机地址，IPv4的loopback范围是127.0.0.0 ~
                                                           // 127.255.255.255
                                && ip.getHostAddress().indexOf(":") == -1) {
                            hostAddress = ip.getHostAddress();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("get host address failed", e);
            }
        }
        return hostAddress;
    }

    public static boolean isLocalAddress(String consumer) {
        return (StringUtils.equals(consumer, "127.0.0.1") || StringUtils.equals(consumer, getLocalHostAddress()));
    }
}
