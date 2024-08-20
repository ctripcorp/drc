package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Created by shiruixin
 * 2024/8/16 14:50
 */
public class MqReplicationQuery extends PageReq {
    private List<Long> srcMhaDbMappingIdList;
    private Set<String> srcTableLikePatterns;
    private Set<String> topicLikePatterns;
    private Integer replicationType;
    private boolean queryOtter;




    public List<Long> getSrcMhaDbMappingIdList() {
        return srcMhaDbMappingIdList;
    }

    public void setSrcMhaDbMappingIdList(List<Long> srcMhaDbMappingIdList) {
        this.srcMhaDbMappingIdList = srcMhaDbMappingIdList;
    }

    public Set<String> getSrcTableLikePatterns() {
        return srcTableLikePatterns;
    }

    public void setSrcTableLikePatterns(Set<String> srcTableLikePatterns) {
        this.srcTableLikePatterns = srcTableLikePatterns;
    }

    public Set<String> getTopicLikePatterns() {
        return topicLikePatterns;
    }

    public void setTopicLikePatterns(Set<String> topicLikePatterns) {
        this.topicLikePatterns = topicLikePatterns;
    }

    public boolean isQueryOtter() {
        return queryOtter;
    }

    public void setQueryOtter(boolean queryOtter) {
        this.queryOtter = queryOtter;
    }

    public void setReplicationType(Integer replicationType) {
        this.replicationType = replicationType;
    }

    public Integer getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(int replicationType) {
        this.replicationType = replicationType;
    }

    public void addOrIntersectSrcMhaDbMappingIds(List<Long> ids) {
        if (this.srcMhaDbMappingIdList == null) {
            this.srcMhaDbMappingIdList = Lists.newArrayList(ids);
        } else {
            this.srcMhaDbMappingIdList.retainAll(ids);
        }
    }

}
