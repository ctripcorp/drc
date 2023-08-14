package com.ctrip.framework.drc.console.check;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.google.common.collect.Maps;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.ClassUtils;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.AllTests.readFileContent;
import static com.ctrip.framework.drc.console.utils.UTConstants.XML_FILE_META_ONLINE;

/**
 * Created by jixinwang on 2023/3/23
 */
//1. call get request http://drc.ctripcorp.com/api/drc/v1/meta/ to get xml config of all meta information
//2. add step 1 meta information to file console/src/test/resources/meta.online.xml(contains sensitive information and cannot be uploaded to the gitlab, has been assume-unchanged)
//3. Execute method checkConfig in this class
public class StandbyConfigCheckTest {

    private List<String> replicatorIpXy;
    private List<String> replicatorIpRb;
    private List<String> applierIpXy;
    private List<String> applierIpRb;
    private List<String> replicatorIpSinB;
    private List<String> replicatorIpSinC;
    private List<String> applierIpSinB;
    private List<String> applierIpSinC;


    @Before
    public void setUp() throws Exception {
        replicatorIpXy = Lists.newArrayList("10.110.60.172", "10.113.60.41", "10.113.60.42", "10.113.93.227", "10.110.60.149", "10.110.60.150", "10.110.60.143", "10.113.93.226", "10.113.93.225", "10.28.156.20", "10.28.200.40");
        replicatorIpRb = Lists.newArrayList("10.60.34.118", "10.60.4.93", "10.60.122.52", "10.58.94.137", "10.60.158.50", "10.60.158.49", "10.60.158.51", "10.58.94.136", "10.61.69.232", "10.60.72.248", "10.60.72.249");
        applierIpXy = Lists.newArrayList("10.113.93.162", "10.113.93.163", "10.113.93.159", "10.113.93.161", "10.113.93.164", "10.113.93.165", "10.113.93.160", "10.113.93.158", "10.113.92.135", "10.113.92.145", "10.113.63.87", "10.113.63.73", "10.113.63.85", "10.113.63.88", "10.113.63.86", "10.113.63.89", "10.113.93.52", "10.113.93.51", "10.113.93.55", "10.113.93.54", "10.113.93.50", "10.113.93.49", "10.113.93.53", "10.113.93.58", "10.113.63.45", "10.113.63.57", "10.113.63.72", "10.113.63.35", "10.113.63.58", "10.113.63.52", "10.113.60.255", "10.113.60.254", "10.109.5.215", "10.112.221.103", "10.110.63.188", "10.110.63.189", "10.110.63.185", "10.110.63.184");
        applierIpRb = Lists.newArrayList("10.60.56.23", "10.60.56.224", "10.60.56.229", "10.60.56.228", "10.60.56.226", "10.60.56.230", "10.60.56.225", "10.60.56.227", "10.60.109.27", "10.60.109.28", "10.60.4.248", "10.60.128.96", "10.60.128.98", "10.60.128.97", "10.60.128.99", "10.60.128.94", "10.60.128.95", "10.60.128.93", "10.60.128.100", "10.60.4.192", "10.60.4.193", "10.60.4.25", "10.60.56.222", "10.60.155.95", "10.60.155.94", "10.60.155.93", "10.60.155.92");

        replicatorIpSinB = Lists.newArrayList("10.129.67.218","10.129.81.165","10.129.84.192","10.129.88.73","10.129.64.27","10.129.92.37", "10.129.94.11", "10.129.65.219");
        replicatorIpSinC = Lists.newArrayList("10.129.70.98","10.129.69.122","10.129.71.128","10.129.71.135","10.129.100.76","10.129.70.183", "10.129.106.17");

        applierIpSinB = Lists.newArrayList("10.129.93.112","10.129.66.146","10.129.85.93","10.129.93.168","10.129.64.57", "10.129.66.242","10.129.92.252", "10.129.67.240","10.129.64.171","10.129.93.80","10.129.67.115","10.129.94.240","10.129.92.36","10.129.64.79","10.129.66.103","10.129.67.178","10.129.66.101","10.129.65.167","10.129.90.35","10.129.67.119","10.129.67.241", "10.129.92.103", "10.129.89.237");
        applierIpSinC = Lists.newArrayList("10.129.69.2","10.129.71.215","10.129.103.248","10.129.71.199","10.129.68.174","10.129.97.203","10.129.104.153", "10.129.71.242", "10.129.101.69","10.129.69.238","10.129.108.18","10.129.70.243","10.129.71.165","10.129.96.169","10.129.71.210","10.129.104.29","10.129.108.248","10.129.108.20","10.129.99.7","10.129.70.240", "10.129.68.86", "10.129.71.130", "10.129.71.145", "10.129.111.32","10.129.98.25", "10.129.100.11", "10.129.69.6");
    }

    @Test
    public void checkConfig() {
        Drc drc = getDrc();
        Map<String, Dc> dcs = drc.getDcs();
        for (Dc dc : dcs.values()) {
            String region = dc.getRegion();
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            for (DbCluster dbCluster : dbClusters.values()) {
                String mhaName = dbCluster.getMhaName();
                List<Replicator> replicators = dbCluster.getReplicators();
                checkReplicator(replicators, region, mhaName);

                List<Messenger> messengers = dbCluster.getMessengers();
                checkMessenger(messengers, region, mhaName);

                List<Applier> appliers = dbCluster.getAppliers();
                checkApplier(appliers, region, mhaName);
            }

        }
    }

    private void checkReplicator(List<Replicator> replicators, String region, String mhaName) {
        if (!replicators.isEmpty()) {
            if (replicators.size() == 2) {
                checkReplicatorInDiffZone(replicators, mhaName, region);
            } else {
                System.out.println(region + "-" + mhaName + ": (" + replicators.size() + " replicator)");
            }
        }
    }

    private void checkReplicatorInDiffZone(List<Replicator> replicators, String mhaName, String region) {
        Replicator r1 = replicators.get(0);
        String r1Ip = r1.getIp();
        Replicator r2 = replicators.get(1);
        String r2Ip = r2.getIp();
        if ("sha".equalsIgnoreCase(region)) {
            if (replicatorIpXy.contains(r1Ip)) {
                if (replicatorIpRb.contains(r2Ip)) {
//                    System.out.println("replicator success for: " + mhaName);
                } else {
                    System.out.println("replicator error for not in sha diff zone: " + mhaName + "----------------");
                }
            } else if (replicatorIpRb.contains(r1Ip)) {
                if (replicatorIpXy.contains(r2Ip)) {
//                    System.out.println("replicator success for: " + mhaName);
                } else {
                    System.out.println("replicator error for not in sha diff zone: " + mhaName + "----------------");
                }
            } else {
                System.out.println("replicator error for not in sha diff zone: " + mhaName + "----------------");
            }
        }

        if ("sin".equalsIgnoreCase(region)) {
            if (replicatorIpSinB.contains(r1Ip)) {
                if (replicatorIpSinC.contains(r2Ip)) {
//                    System.out.println("replicator success for: " + mhaName);
                } else {
                    System.out.println("replicator error for not in sin diff zone: " + mhaName + "----------------");
                }
            } else if (replicatorIpSinC.contains(r1Ip)) {
                if (replicatorIpSinB.contains(r2Ip)) {
//                    System.out.println("replicator success for: " + mhaName);
                } else {
                    System.out.println("replicator error for not in sin diff zone: " + mhaName + "----------------");
                }
            } else {
                System.out.println("replicator error for not in sin diff zone: " + mhaName + "----------------");
            }
        }
    }

    private void checkMessenger(List<Messenger> messengers, String region, String mhaName) {
        if (!messengers.isEmpty()) {
            if (messengers.size() == 2) {
                checkMessengerInDiffZone(messengers, mhaName, region);
            } else {
                System.out.println(region + "-" + mhaName + ": (" + messengers.size() + " messenger)");
            }
        }
    }

    private void checkMessengerInDiffZone(List<Messenger> messengers, String mhaName, String region) {
        Messenger m1 = messengers.get(0);
        String m1Ip = m1.getIp();
        Messenger m2 = messengers.get(1);
        String m2Ip = m2.getIp();
        if ("sha".equalsIgnoreCase(region)) {
            if (applierIpXy.contains(m1Ip)) {
                if (applierIpRb.contains(m2Ip)) {
//                    System.out.println("messenger success for: " + mhaName);
                } else {
                    System.out.println("messenger error for not in sha diff zone: " + mhaName + "----------------");
                }
            } else if (applierIpRb.contains(m1Ip)) {
                if (applierIpXy.contains(m2Ip)) {
//                    System.out.println("messenger success for: " + mhaName);
                } else {
                    System.out.println("messenger error for not in sha diff zone: " + mhaName + "----------------");
                }
            } else {
                System.out.println("messenger error for not in sha diff zone: " + mhaName + "----------------");
            }
        }

        if ("sin".equalsIgnoreCase(region)) {
            if (applierIpSinB.contains(m1Ip)) {
                if (applierIpSinC.contains(m2Ip)) {
//                    System.out.println("messenger success for: " + mhaName);
                } else {
                    System.out.println("messenger error for not in sin diff zone: " + mhaName + "----------------");
                }
            } else if (applierIpSinC.contains(m1Ip)) {
                if (applierIpSinB.contains(m2Ip)) {
//                    System.out.println("messenger success for: " + mhaName);
                } else {
                    System.out.println("messenger error for not in sin diff zone: " + mhaName + "----------------");
                }
            } else {
                System.out.println("messenger error for not in sin diff zone: " + mhaName + "----------------");
            }
        }
    }

    private void checkApplier(List<Applier> appliers, String region, String mhaName) {
        if (!appliers.isEmpty()) {
            Map<String, List<String>> targetMhaAndApplierIpMap = getTargetMhaAndApplierIpMap(appliers);
            for (Map.Entry<String, List<String>> entry : targetMhaAndApplierIpMap.entrySet()) {
                String targetMhaName = entry.getKey();
                List<String> applierIps = entry.getValue();
                if (applierIps.size() == 2) {
                    checkApplierInDiffZone(applierIps, mhaName, region, targetMhaName);
                } else {
                    System.out.println(region + "-" + mhaName + ": (" + applierIps.size() + " applier)-targetMha: " + targetMhaName);
                }
            }
        }
    }

    private void checkApplierInDiffZone(List<String> applierIps, String mhaName, String region, String targetMhaName) {
        String a1Ip = applierIps.get(0);
        String a2Ip = applierIps.get(1);

        if ("sha".equalsIgnoreCase(region)) {
            if (applierIpXy.contains(a1Ip)) {
                if (applierIpRb.contains(a2Ip)) {
//                    System.out.println("applier success for: " + mhaName);
                } else {
                    System.out.println("applier error for not in sha diff zone: " + mhaName + "----------------, targetMhaName: " + targetMhaName);
                }
            } else if (applierIpRb.contains(a1Ip)) {
                if (applierIpXy.contains(a2Ip)) {
//                    System.out.println("applier success for: " + mhaName);
                } else {
                    System.out.println("applier error for not in sha diff zone: " + mhaName + "----------------, targetMhaName: " + targetMhaName);
                }
            } else {
                System.out.println("applier error for not in sha diff zone: " + mhaName + "----------------, targetMhaName: " + targetMhaName);
            }
        }

        if ("sin".equalsIgnoreCase(region)) {
            if (applierIpSinB.contains(a1Ip)) {
                if (applierIpSinC.contains(a2Ip)) {
//                    System.out.println("applier success for: " + mhaName);
                } else {
                    System.out.println("applier error for not in sin diff zone: " + mhaName + "----------------, targetMhaName: " + targetMhaName);
                }
            } else if (applierIpSinC.contains(a1Ip)) {
                if (applierIpSinB.contains(a2Ip)) {
//                    System.out.println("applier success for: " + mhaName);
                } else {
                    System.out.println("applier error for not in sin diff zone: " + mhaName + "----------------, targetMhaName: " + targetMhaName);
                }
            } else {
                System.out.println("applier error for not in sin diff zone: " + mhaName + "----------------, targetMhaName: " + targetMhaName);
            }
        }
    }

    public Map<String, List<String>> getTargetMhaAndApplierIpMap(List<Applier> appliers) {
        Map<String, List<String>> targetMhaAndApplierIpMap = Maps.newHashMap();
        for (Applier applier : appliers) {
            String targetMha = applier.getTargetMhaName();
            List<String> applierIps = targetMhaAndApplierIpMap.get(targetMha);
            if (applierIps == null) {
                applierIps = Lists.newArrayList();
                applierIps.add(applier.getIp());
                targetMhaAndApplierIpMap.put(targetMha, applierIps);
            } else {
                applierIps.add(applier.getIp());
            }
        }
        return targetMhaAndApplierIpMap;
    }

    private Drc getDrc() {
        String file = ClassUtils.getDefaultClassLoader().getResource(XML_FILE_META_ONLINE).getPath();
        String drcXmlOnline = readFileContent(file);
        Drc drc = null;
        try {
            drc = DefaultSaxParser.parse(drcXmlOnline);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {

        }
        return drc;
    }
}
