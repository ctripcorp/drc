package com.ctrip.framework.drc.console.vo.display;


import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.util.List;

public class MigrationTaskVo {
    private Long id;
    private List<String> dbs;
    private String oldMha;
    private String newMha;
    private String oldMhaDba;
    private String newMhaDba;
    private String status;
    private String operator;
    private String log;

    @Override
    public String toString() {
        return "MigrationTaskVo{" +
                "id=" + id +
                ", dbs=" + dbs +
                ", oldMha='" + oldMha + '\'' +
                ", newMha='" + newMha + '\'' +
                ", oldMhaDba='" + oldMhaDba + '\'' +
                ", newMhaDba='" + newMhaDba + '\'' +
                ", status='" + status + '\'' +
                ", operator='" + operator + '\'' +
                ", log='" + log + '\'' +
                '}';
    }

    public String getOldMhaDba() {
        return oldMhaDba;
    }

    public void setOldMhaDba(String oldMhaDba) {
        this.oldMhaDba = oldMhaDba;
    }

    public String getNewMhaDba() {
        return newMhaDba;
    }

    public void setNewMhaDba(String newMhaDba) {
        this.newMhaDba = newMhaDba;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }

    public String getOldMha() {
        return oldMha;
    }

    public void setOldMha(String oldMha) {
        this.oldMha = oldMha;
    }

    public String getNewMha() {
        return newMha;
    }

    public void setNewMha(String newMha) {
        this.newMha = newMha;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public static MigrationTaskVo from(MigrationTaskTbl tbl) {
        MigrationTaskVo vo = new MigrationTaskVo();
        vo.setId(tbl.getId());
        vo.setDbs(JsonUtils.fromJsonToList(tbl.getDbs(), String.class));
        vo.setOldMha(tbl.getOldMha());
        vo.setNewMha(tbl.getNewMha());
        vo.setStatus(tbl.getStatus());
        vo.setOperator(tbl.getOperator());
        vo.setNewMhaDba(tbl.getNewMhaDba());
        vo.setNewMhaDba(tbl.getOldMhaDba());
        vo.setLog(tbl.getLog());
        return vo;


    }
}
