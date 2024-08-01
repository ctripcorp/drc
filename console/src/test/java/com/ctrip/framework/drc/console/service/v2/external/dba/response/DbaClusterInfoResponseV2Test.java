package com.ctrip.framework.drc.console.service.v2.external.dba.response;

import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

/**
 * @author yongnian
 * @create 2024/8/1 14:05
 */
public class DbaClusterInfoResponseV2Test {


    @Test
    public void testCompatibility() {
        DbaClusterInfoResponseV2 v2 = JsonUtils.fromJson(getV2Json(), DbaClusterInfoResponseV2.class);
        DbaClusterInfoResponse v1Before = JsonUtils.fromJson(getV1Json(), DbaClusterInfoResponse.class);
        DbaClusterInfoResponse v1After = v2.toV1();

        Assert.assertEquals(v1Before.getSuccess(), v1After.getSuccess());

        // compare data
        List<MemberInfo> memberlistBefore = v1Before.getData().getMemberlist();
        List<MemberInfo> memberlistAfter = v1After.getData().getMemberlist();
        Assert.assertEquals(memberlistBefore.size(), memberlistAfter.size());
        memberlistBefore.sort(Comparator.comparing(MemberInfo::getService_ip));
        memberlistAfter.sort(Comparator.comparing(MemberInfo::getService_ip));
        Assert.assertEquals(memberlistBefore.size(), memberlistAfter.size());

        for (int i = 0; i < memberlistBefore.size(); i++) {
            MemberInfo before = memberlistBefore.get(i);
            MemberInfo after = memberlistAfter.get(i);
            Assert.assertEquals(before.getService_ip(), after.getService_ip());
            Assert.assertEquals(before.getStatus(), after.getStatus());
            Assert.assertEquals(before.getDns_port(), after.getDns_port());
            Assert.assertEquals(before.getRole(), after.getRole());
            Assert.assertEquals(before.getMachine_located_short(), after.getMachine_located_short());
        }
    }

    public String getV2Json() {
        return "{\n" +
                "  \"message\": \"ok\",\n" +
                "  \"success\": true,\n" +
                "  \"data\": [\n" +
                "    {\n" +
                "      \"clusterName\": \"bbzhistoricalorderindex102\",\n" +
                "      \"machineName\": \"SVR33106IN5112\",\n" +
                "      \"ipBusiness\": \"10.109.216.82\",\n" +
                "      \"clusterPort\": 55945,\n" +
                "      \"role\": \"master\",\n" +
                "      \"status\": \"online\",\n" +
                "      \"dataDir\": \"/data/mysql55945/\",\n" +
                "      \"version\": \"8.0.36\",\n" +
                "      \"machineLocated\": \"上海新源IDC(移动)\",\n" +
                "      \"zone\": \"shaxy\",\n" +
                "      \"OSVersion\": \"AlmaLinux release 9.1 (Lime Lynx)\",\n" +
                "      \"insertTime\": \"2024-07-25T12:40:13+08:00\",\n" +
                "      \"modifyTime\": \"2024-08-01T13:40:20+08:00\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"clusterName\": \"bbzhistoricalorderindex102\",\n" +
                "      \"machineName\": \"SVR32258IN5112\",\n" +
                "      \"ipBusiness\": \"10.58.235.208\",\n" +
                "      \"clusterPort\": 55945,\n" +
                "      \"role\": \"slave\",\n" +
                "      \"status\": \"online\",\n" +
                "      \"dataDir\": \"/data/mysql55945/\",\n" +
                "      \"version\": \"8.0.36\",\n" +
                "      \"machineLocated\": \"上海日阪IDC(联通)\",\n" +
                "      \"zone\": \"sharb\",\n" +
                "      \"OSVersion\": \"AlmaLinux release 9.1 (Lime Lynx)\",\n" +
                "      \"insertTime\": \"2024-07-25T12:40:13+08:00\",\n" +
                "      \"modifyTime\": \"2024-08-01T13:40:20+08:00\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"clusterName\": \"bbzhistoricalorderindex102\",\n" +
                "      \"machineName\": \"VMSALI838026\",\n" +
                "      \"ipBusiness\": \"10.25.224.150\",\n" +
                "      \"clusterPort\": 55945,\n" +
                "      \"role\": \"slave-dr\",\n" +
                "      \"status\": \"online\",\n" +
                "      \"dataDir\": \"/data/mysql55945/\",\n" +
                "      \"version\": \"8.0.36\",\n" +
                "      \"machineLocated\": \"公有云(SHA-ALI)\",\n" +
                "      \"zone\": \"sha-ali\",\n" +
                "      \"OSVersion\": \"Alibaba Cloud Linux release 3 (Soaring Falcon)\",\n" +
                "      \"insertTime\": \"2024-07-25T16:40:13+08:00\",\n" +
                "      \"modifyTime\": \"2024-08-01T13:40:20+08:00\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }

    public String getV1Json() {
        return "{\n" +
                "  \"message\": \"success\",\n" +
                "  \"success\": true,\n" +
                "  \"data\": {\n" +
                "    \"memberlist\": [\n" +
                "      {\n" +
                "        \"status\": \"online\",\n" +
                "        \"mastervip\": \"\",\n" +
                "        \"machine_name\": \"SVR32258IN5112\",\n" +
                "        \"machine_located_short\": \"SHARB\",\n" +
                "        \"service_ip\": \"10.58.235.208\",\n" +
                "        \"cluster_name\": \"bbzhistoricalorderindex102\",\n" +
                "        \"dns_port\": 55945,\n" +
                "        \"datadir\": \"/data/mysql55945/\",\n" +
                "        \"machine_located\": \"\\u4e0a\\u6d77\\u65e5\\u962aIDC(\\u8054\\u901a)\",\n" +
                "        \"role\": \"slave\",\n" +
                "        \"modify_time\": \"2024-07-25 13:56:23.544000\",\n" +
                "        \"ip_business_gateway\": \"10.58.232.1\",\n" +
                "        \"ip_business\": \"10.58.235.208\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"status\": \"online\",\n" +
                "        \"mastervip\": \"\",\n" +
                "        \"machine_name\": \"SVR33106IN5112\",\n" +
                "        \"machine_located_short\": \"SHAXY\",\n" +
                "        \"service_ip\": \"10.109.216.82\",\n" +
                "        \"cluster_name\": \"bbzhistoricalorderindex102\",\n" +
                "        \"dns_port\": 55945,\n" +
                "        \"datadir\": \"/data/mysql55945/\",\n" +
                "        \"machine_located\": \"\\u4e0a\\u6d77\\u65b0\\u6e90IDC(\\u79fb\\u52a8)\",\n" +
                "        \"role\": \"master\",\n" +
                "        \"modify_time\": \"2024-07-25 12:19:26.031000\",\n" +
                "        \"ip_business_gateway\": \"10.109.216.1\",\n" +
                "        \"ip_business\": \"10.109.216.82\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"status\": \"online\",\n" +
                "        \"mastervip\": \"\",\n" +
                "        \"machine_name\": \"VMSALI838026\",\n" +
                "        \"machine_located_short\": \"SHA-ALI\",\n" +
                "        \"service_ip\": \"10.25.224.150\",\n" +
                "        \"cluster_name\": \"bbzhistoricalorderindex102\",\n" +
                "        \"dns_port\": 55945,\n" +
                "        \"datadir\": \"/data/mysql55945/\",\n" +
                "        \"machine_located\": \"\\u516c\\u6709\\u4e91(SHA-ALI)\",\n" +
                "        \"role\": \"slave-dr\",\n" +
                "        \"modify_time\": \"2024-07-25 16:33:48.684000\",\n" +
                "        \"ip_business_gateway\": \"10.25.227.253\",\n" +
                "        \"ip_business\": \"10.25.224.150\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }
}
