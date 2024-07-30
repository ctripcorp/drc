package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;

public class MachineDto {
    private Integer port;
    private String ip;
    private Boolean isMaster;

    public MachineDto() {
    }
    
    public MachineDto(Integer port, String ip, Boolean isMaster) {
        this.port = port;
        this.ip = ip;
        this.isMaster = isMaster;
    }

    public Integer getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public Boolean getMaster() {
        return isMaster;
    }
    public static MachineDto from(ClusterInfoDto.Node node) {
        MachineDto machineDto = new MachineDto();
        machineDto.ip = node.getIpBusiness();
        machineDto.isMaster = node.getRole().toLowerCase().contains("master");
        machineDto.port = node.getInstancePort();
        return machineDto;
    }

}
