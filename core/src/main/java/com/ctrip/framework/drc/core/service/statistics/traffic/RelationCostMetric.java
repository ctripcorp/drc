package com.ctrip.framework.drc.core.service.statistics.traffic;

/**
 * Created by jixinwang on 2022/11/9
 */
public class RelationCostMetric {

    private Long timestamp;
    private String service_type;
    private String cost_group;
    private String refered_service_type;
    private String refered_app_instance;
    private String relation_group;
    private String parent_relation_group;
    private String _schema_version;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getService_type() {
        return service_type;
    }

    public void setService_type(String service_type) {
        this.service_type = service_type;
    }

    public String getCost_group() {
        return cost_group;
    }

    public void setCost_group(String cost_group) {
        this.cost_group = cost_group;
    }

    public String getRefered_service_type() {
        return refered_service_type;
    }

    public void setRefered_service_type(String refered_service_type) {
        this.refered_service_type = refered_service_type;
    }

    public String getRefered_app_instance() {
        return refered_app_instance;
    }

    public void setRefered_app_instance(String refered_app_instance) {
        this.refered_app_instance = refered_app_instance;
    }

    public String getRelation_group() {
        return relation_group;
    }

    public void setRelation_group(String relation_group) {
        this.relation_group = relation_group;
    }

    public String getParent_relation_group() {
        return parent_relation_group;
    }

    public void setParent_relation_group(String parent_relation_group) {
        this.parent_relation_group = parent_relation_group;
    }

    public String get_schema_version() {
        return _schema_version;
    }

    public void set_schema_version(String _schema_version) {
        this._schema_version = _schema_version;
    }
}
