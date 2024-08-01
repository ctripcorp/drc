package com.ctrip.framework.drc.console.service.v2.external.dba.response;


public class MemberInfoV2 {
    private String clusterName;
    private String machineName;
    private String ipBusiness;
    private int clusterPort;
    private String role;
    private String status;
    private String dataDir;
    private String version;
    private String machineLocated;
    private String zone;
    private String OSVersion;

    public MemberInfo toV1() {
        MemberInfo memberInfo = new MemberInfo();
        memberInfo.setStatus(this.status);
        memberInfo.setMachine_name(this.machineName);
        memberInfo.setMachine_located_short(this.zone.toUpperCase());
        memberInfo.setService_ip(this.ipBusiness);
        memberInfo.setCluster_name(this.clusterName);
        memberInfo.setDns_port(this.clusterPort);
        memberInfo.setDatadir(this.dataDir);
        memberInfo.setMachine_located(this.machineLocated);
        memberInfo.setRole(this.role);
        memberInfo.setIp_business(this.ipBusiness);

        memberInfo.setMastervip(null);
        memberInfo.setModify_time(null);
        memberInfo.setIp_business_gateway(null);
        return memberInfo;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getMachineName() {
        return machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    public String getIpBusiness() {
        return ipBusiness;
    }

    public void setIpBusiness(String ipBusiness) {
        this.ipBusiness = ipBusiness;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public void setClusterPort(int clusterPort) {
        this.clusterPort = clusterPort;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getMachineLocated() {
        return machineLocated;
    }

    public void setMachineLocated(String machineLocated) {
        this.machineLocated = machineLocated;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getOSVersion() {
        return OSVersion;
    }

    public void setOSVersion(String OSVersion) {
        this.OSVersion = OSVersion;
    }
}