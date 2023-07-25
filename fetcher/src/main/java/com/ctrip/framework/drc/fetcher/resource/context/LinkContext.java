package com.ctrip.framework.drc.fetcher.resource.context;

/**
 * @Author Slight
 * Sep 27, 2019
 */
public interface LinkContext extends
        TimeContext,
        TableKeyContext,
        TableKeyMapContext,
        SchemaContext,
        SchemasHistoryContext,
        EventGroupContext {
}
