package com.ctrip.framework.drc.console.service.v2.external.dba.response;


import com.ctrip.framework.drc.console.dto.v2.MachineDto;

public class MemberInfo {
    private String status;
    private String mastervip;
    private String machine_name;
    private String machine_located_short;
    private String service_ip;
    private String cluster_name;
    private int dns_port;
    private String datadir;
    private String machine_located;
    private String role;
    private String modify_time;
    private String ip_business_gateway;
    private String ip_business;
    public void setStatus(String status) {
        this.status = status;
    }
    public String getStatus() {
        return status;
    }

    public void setMastervip(String mastervip) {
        this.mastervip = mastervip;
    }
    public String getMastervip() {
        return mastervip;
    }

    public void setMachine_name(String machine_name) {
        this.machine_name = machine_name;
    }
    public String getMachine_name() {
        return machine_name;
    }

    public void setMachine_located_short(String machine_located_short) {
        this.machine_located_short = machine_located_short;
    }
    public String getMachine_located_short() {
        return machine_located_short;
    }

    public void setService_ip(String service_ip) {
        this.service_ip = service_ip;
    }
    public String getService_ip() {
        return service_ip;
    }

    public void setCluster_name(String cluster_name) {
        this.cluster_name = cluster_name;
    }
    public String getCluster_name() {
        return cluster_name;
    }

    public void setDns_port(int dns_port) {
        this.dns_port = dns_port;
    }
    public int getDns_port() {
        return dns_port;
    }

    public void setDatadir(String datadir) {
        this.datadir = datadir;
    }
    public String getDatadir() {
        return datadir;
    }

    public void setMachine_located(String machine_located) {
        this.machine_located = machine_located;
    }
    public String getMachine_located() {
        return machine_located;
    }

    public void setRole(String role) {
        this.role = role;
    }
    public String getRole() {
        return role;
    }

    public void setModify_time(String modify_time) {
        this.modify_time = modify_time;
    }
    public String getModify_time() {
        return modify_time;
    }

    public void setIp_business_gateway(String ip_business_gateway) {
        this.ip_business_gateway = ip_business_gateway;
    }
    public String getIp_business_gateway() {
        return ip_business_gateway;
    }

    public void setIp_business(String ip_business) {
        this.ip_business = ip_business;
    }
    public String getIp_business() {
        return ip_business;
    }

    public static MachineDto toMachineDto(MemberInfo memberInfo) {
        return new MachineDto(
            memberInfo.getDns_port(),
            memberInfo.getService_ip(),
            memberInfo.getRole().toLowerCase().contains("master")
        );
    }
}