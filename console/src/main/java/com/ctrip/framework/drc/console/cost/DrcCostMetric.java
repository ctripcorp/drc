package com.ctrip.framework.drc.console.cost;

import java.util.Objects;

/**
 * Created by jixinwang on 2022/9/6
 */
public class DrcCostMetric {

    private Long timestamp;
    private String cloud_provider;
    private String region;
    private String zone;
    private String app_name;
    private String service_type;
    private String app_platform;
    private String app_instance;
    private String operation;
    private Float share_unit_type;
    private String cost_group;
    private String owner;
    private String bu_code;
    private String product_line_code;


    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCloud_provider() {
        return cloud_provider;
    }

    public void setCloud_provider(String cloud_provider) {
        this.cloud_provider = cloud_provider;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getService_type() {
        return service_type;
    }

    public void setService_type(String service_type) {
        this.service_type = service_type;
    }

    public String getApp_platform() {
        return app_platform;
    }

    public void setApp_platform(String app_platform) {
        this.app_platform = app_platform;
    }

    public String getApp_instance() {
        return app_instance;
    }

    public void setApp_instance(String app_instance) {
        this.app_instance = app_instance;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Float getShare_unit_type() {
        return share_unit_type;
    }

    public void setShare_unit_type(Float share_unit_type) {
        this.share_unit_type = share_unit_type;
    }

    public String getCost_group() {
        return cost_group;
    }

    public void setCost_group(String cost_group) {
        this.cost_group = cost_group;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getBu_code() {
        return bu_code;
    }

    public void setBu_code(String bu_code) {
        this.bu_code = bu_code;
    }

    public String getProduct_line_code() {
        return product_line_code;
    }

    public void setProduct_line_code(String product_line_code) {
        this.product_line_code = product_line_code;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DrcCostMetric that = (DrcCostMetric) o;
        return Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(cloud_provider, that.cloud_provider) &&
                Objects.equals(region, that.region) &&
                Objects.equals(zone, that.zone) &&
                Objects.equals(app_name, that.app_name) &&
                Objects.equals(service_type, that.service_type) &&
                Objects.equals(app_platform, that.app_platform) &&
                Objects.equals(app_instance, that.app_instance) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(share_unit_type, that.share_unit_type) &&
                Objects.equals(cost_group, that.cost_group) &&
                Objects.equals(owner, that.owner) &&
                Objects.equals(bu_code, that.bu_code) &&
                Objects.equals(product_line_code, that.product_line_code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, cloud_provider, region, zone, app_name, service_type, app_platform, app_instance, operation, share_unit_type, cost_group, owner, bu_code, product_line_code);
    }

    @Override
    public String toString() {
        return "DrcCostMetric{" +
                "timestamp=" + timestamp +
                ", cloud_provider='" + cloud_provider + '\'' +
                ", region='" + region + '\'' +
                ", zone='" + zone + '\'' +
                ", app_name='" + app_name + '\'' +
                ", service_type='" + service_type + '\'' +
                ", app_platform='" + app_platform + '\'' +
                ", app_instance='" + app_instance + '\'' +
                ", operation='" + operation + '\'' +
                ", share_unit_type=" + share_unit_type +
                ", cost_group='" + cost_group + '\'' +
                ", owner='" + owner + '\'' +
                ", bu_code='" + bu_code + '\'' +
                ", product_line_code='" + product_line_code + '\'' +
                '}';
    }
}
