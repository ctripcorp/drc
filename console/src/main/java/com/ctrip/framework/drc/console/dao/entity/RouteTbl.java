package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2021-05-07
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "route_tbl")
public class RouteTbl implements DalPojo {

    /**
     * primary key
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * organization id of route
     */
    @Column(name = "route_org_id")
    @Type(value = Types.BIGINT)
    private Long routeOrgId;

    /**
     * source dc id
     */
    @Column(name = "src_dc_id")
    @Type(value = Types.BIGINT)
    private Long srcDcId;

    /**
     * destination dc id
     */
    @Column(name = "dst_dc_id")
    @Type(value = Types.BIGINT)
    private Long dstDcId;

    /**
     * source proxies ids
     */
    @Column(name = "src_proxy_ids")
    @Type(value = Types.VARCHAR)
    private String srcProxyIds;

    /**
     * destination proxies ids
     */
    @Column(name = "dst_proxy_ids")
    @Type(value = Types.VARCHAR)
    private String dstProxyIds;

    /**
     * tag for console or meta
     */
    @Column(name = "tag")
    @Type(value = Types.VARCHAR)
    private String tag;

    /**
     * deleted or not
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * globalActive
     */
    @Column(name = "global_active")
    @Type(value = Types.TINYINT)
    private Integer globalActive;

    /**
     * create time
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * data changed last time
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    /**
     * optional relay proxies
     */
    @Column(name = "optional_proxy_ids")
    @Type(value = Types.VARCHAR)
    private String optionalProxyIds;

    public static RouteTbl createRoutePojo(Long routeOrgId, Long srcDcId, Long dstDcId, String srcProxyIds, String relayProxyIds, String dstProxyIds, String tag) {
        RouteTbl routeTbl = new RouteTbl();
        routeTbl.setRouteOrgId(routeOrgId);
        routeTbl.setSrcDcId(srcDcId);
        routeTbl.setDstDcId(dstDcId);
        routeTbl.setSrcProxyIds(srcProxyIds);
        routeTbl.setOptionalProxyIds(relayProxyIds);
        routeTbl.setDstProxyIds(dstProxyIds);
        routeTbl.setTag(tag);
        routeTbl.setGlobalActive(0);
        return routeTbl;
    }

    public Integer getGlobalActive() {
        return globalActive;
    }

    public void setGlobalActive(Integer globalActive) {
        this.globalActive = globalActive;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getRouteOrgId() {
        return routeOrgId;
    }

    public void setRouteOrgId(Long routeOrgId) {
        this.routeOrgId = routeOrgId;
    }

    public Long getSrcDcId() {
        return srcDcId;
    }

    public void setSrcDcId(Long srcDcId) {
        this.srcDcId = srcDcId;
    }

    public Long getDstDcId() {
        return dstDcId;
    }

    public void setDstDcId(Long dstDcId) {
        this.dstDcId = dstDcId;
    }

    public String getSrcProxyIds() {
        return srcProxyIds;
    }

    public void setSrcProxyIds(String srcProxyIds) {
        this.srcProxyIds = srcProxyIds;
    }

    public String getDstProxyIds() {
        return dstProxyIds;
    }

    public void setDstProxyIds(String dstProxyIds) {
        this.dstProxyIds = dstProxyIds;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
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

    public String getOptionalProxyIds() {
        return optionalProxyIds;
    }

    public void setOptionalProxyIds(String optionalProxyIds) {
        this.optionalProxyIds = optionalProxyIds;
    }

    public boolean equalRoute(RouteTbl other) {
        return routeOrgId.equals(other.routeOrgId)
                && srcDcId.equals(other.srcDcId)
                && dstDcId.equals(other.dstDcId)
                && srcProxyIds.equals(other.srcProxyIds)
                && dstProxyIds.equals(other.dstProxyIds)
                && optionalProxyIds.equals(other.optionalProxyIds)
                && tag.equals(other.tag);
    }

}