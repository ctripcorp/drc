package com.ctrip.framework.drc.core.server.config.replicator.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicatorDetailInfoDto {
    private String registryKey;
    private Map<String, List<ScannerDto>> scannerMap = new HashMap<>();
    private String purgedGtidSet;
    private String executedGtidSet;

    private String oldestBinlogFile;
    private String latestBinlogFile;

    public String getOldestBinlogFile() {
        return oldestBinlogFile;
    }

    public void setOldestBinlogFile(String oldestBinlogFile) {
        this.oldestBinlogFile = oldestBinlogFile;
    }

    public String getLatestBinlogFile() {
        return latestBinlogFile;
    }

    public void setLatestBinlogFile(String latestBinlogFile) {
        this.latestBinlogFile = latestBinlogFile;
    }

    public String getPurgedGtidSet() {
        return purgedGtidSet;
    }

    public void setPurgedGtidSet(String purgedGtidSet) {
        this.purgedGtidSet = purgedGtidSet;
    }

    public String getExecutedGtidSet() {
        return executedGtidSet;
    }

    public void setExecutedGtidSet(String executedGtidSet) {
        this.executedGtidSet = executedGtidSet;
    }


    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public void addScanner(ScannerDto scannerDto) {
        List<ScannerDto> scannerDtos = scannerMap.computeIfAbsent(scannerDto.getConsumeType(), k -> new ArrayList<>());
        scannerDtos.add(scannerDto);
    }

    public Map<String, List<ScannerDto>> getScannerMap() {
        return scannerMap;
    }

    public static class ScannerDto {
        private String consumeType;
        private List<SenderDto> senders;
        private String gtid;
        private String currentFile;

        public void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }

        public String getCurrentFile() {
            return currentFile;
        }

        public void setConsumeType(String consumeType) {
            this.consumeType = consumeType;
        }

        public void setSenders(List<SenderDto> senders) {
            this.senders = senders;
        }


        public void setGtid(String gtid) {
            this.gtid = gtid;
        }

        public String getGtid() {
            return gtid;
        }

        public String getConsumeType() {
            return consumeType;
        }

        public List<SenderDto> getSenders() {
            return senders;
        }

        public ScannerDto() {
        }
    }

    public static class SenderDto {
        private String name;
        private String gtid;
        private String gtidGap;

        public String getGtid() {
            return gtid;
        }

        public String getGtidGap() {
            return gtidGap;
        }

        public void setGtidGap(String gtidGap) {
            this.gtidGap = gtidGap;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setGtid(String gtid) {
            this.gtid = gtid;
        }

        public SenderDto(String name) {
            this.name = name;
        }
    }
}
