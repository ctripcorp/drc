package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2024/1/31 16:35
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "application_approval_tbl")
public class ApplicationApprovalTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 申请单id
     */
    @Id
    @Column(name = "application_form_id")
    @Type(value = Types.BIGINT)
    private Long applicationFormId;

    /**
     * 审批结果 0-审批中 1-审批通过 2-审批未通过 3-未审批
     */
    @Column(name = "approval_result")
    @Type(value = Types.TINYINT)
    private Integer approvalResult;

    /**
     * 申请人
     */
    @Column(name = "applicant")
    @Type(value = Types.VARCHAR)
    private String applicant;

    /**
     * 审批人
     */
    @Column(name = "operator")
    @Type(value = Types.VARCHAR)
    private String operator;

    /**
     * 备注
     */
    @Column(name = "remark")
    @Type(value = Types.VARCHAR)
    private String remark;

    /**
     * 是否删除, 0-否; 1-是
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * 创建时间
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getApplicationFormId() {
        return applicationFormId;
    }

    public void setApplicationFormId(Long applicationFormId) {
        this.applicationFormId = applicationFormId;
    }

    public Integer getApprovalResult() {
        return approvalResult;
    }

    public void setApprovalResult(Integer approvalResult) {
        this.approvalResult = approvalResult;
    }

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}
