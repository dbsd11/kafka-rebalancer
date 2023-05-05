package group.bison.kafka.rebalancer.utils;

import java.net.InetAddress;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InetUtils {
    
    private static String hostName = "";

    public static String getLocalHostAddress() {
        if(StringUtils.isEmpty(hostName)) {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                hostName = addr.getHostAddress(); //获取本机计算机名称
            } catch (Exception e) {
                log.warn("get hostName failed", e);
            }
        }
        return hostName;
    }
}
