package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author yongnian
 * @create 2024/11/4 11:36
 */
public class ClusterServerInfoTest {

    String oldJson = "{\"ip\":\"ip1\",\"port\":8080}";

    String newJson = "{\"ip\":\"ip1\",\"port\":8080,\"state\":0}";

    // new code, old json
    @Test
    public void testParseOld() {
        // decode old
        ClusterServerInfo decode = Codec.DEFAULT.decode(oldJson, ClusterServerInfo.class);
        Assert.assertEquals("ip1", decode.getIp());
        Assert.assertEquals(8080, decode.getPort());
        Assert.assertEquals(0, decode.getState());
    }

    // new code, new json
    @Test
    public void testEncodeDecode() {
        // encode new
        ClusterServerInfo clusterServerInfo = new ClusterServerInfo("ip1", 8080, ServerStateEnum.NORMAL);
        String encode = Codec.DEFAULT.encode(clusterServerInfo);
        Assert.assertEquals(newJson, encode);

        // decode new
        ClusterServerInfo decode = Codec.DEFAULT.decode(encode, ClusterServerInfo.class);
        Assert.assertEquals("ip1", decode.getIp());
        Assert.assertEquals(8080, decode.getPort());
        Assert.assertEquals(0, decode.getState());
        Assert.assertEquals(ServerStateEnum.NORMAL, decode.getStateEnum());

        // decode new2
        clusterServerInfo = new ClusterServerInfo("ip2", 8081, ServerStateEnum.RESTARTING);
        encode = Codec.DEFAULT.encode(clusterServerInfo);

        decode = Codec.DEFAULT.decode(encode, ClusterServerInfo.class);
        Assert.assertEquals("ip2", decode.getIp());
        Assert.assertEquals(8081, decode.getPort());
        Assert.assertEquals(2, decode.getState());
        Assert.assertEquals(ServerStateEnum.RESTARTING, decode.getStateEnum());
    }

    // old code, new json
    @Test
    public void testEncodeDecodeOld() {
        // encode new
        ClusterServerInfo clusterServerInfo = new ClusterServerInfo("ip1", 8080, ServerStateEnum.NORMAL);
        String encode = Codec.DEFAULT.encode(clusterServerInfo);
        Assert.assertEquals(newJson, encode);

        // decode new using old code
        ClusterServerInfoOld decode = Codec.DEFAULT.decode(encode, ClusterServerInfoOld.class);
        Assert.assertEquals("ip1", decode.getIp());
        Assert.assertEquals(8080, decode.getPort());

        // decode new2 using old code
        clusterServerInfo = new ClusterServerInfo("ip2", 8081, ServerStateEnum.RESTARTING);
        encode = Codec.DEFAULT.encode(clusterServerInfo);

        decode = Codec.DEFAULT.decode(encode, ClusterServerInfoOld.class);
        Assert.assertEquals("ip2", decode.getIp());
        Assert.assertEquals(8081, decode.getPort());
    }


    static class ClusterServerInfoOld {
        private String ip;
        private int port;

        public ClusterServerInfoOld() {

        }

        public ClusterServerInfoOld(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }


        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public boolean equals(Object obj) {

            if (!(obj instanceof com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo)) {
                return false;
            }

            ClusterServerInfoOld other = (ClusterServerInfoOld) obj;
            return ObjectUtils.equals(ip, other.ip) && ObjectUtils.equals(port, other.port);
        }

        @Override
        public int hashCode() {

            int hash = 0;

            hash = hash * 31 + (ip == null ? 0 : ip.hashCode());
            hash = hash * 31 + (port);
            return hash;
        }

        @Override
        public String toString() {
            return Codec.DEFAULT.encode(this);
        }
    }
}