package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/3/1 16:50
 */
public class ResourceMigrateParam {
    List<ResourceMigrateDto> resourceMigrateDtoList;
    private int type;

    public ResourceMigrateParam() {
    }


    public List<ResourceMigrateDto> getResourceMigrateDtoList() {
        return resourceMigrateDtoList;
    }

    public void setResourceMigrateDtoList(List<ResourceMigrateDto> resourceMigrateDtoList) {
        this.resourceMigrateDtoList = resourceMigrateDtoList;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "ResourceMigrateParam{" +
                "resourceMigrateDtoList=" + resourceMigrateDtoList +
                ", type=" + type +
                '}';
    }
}
