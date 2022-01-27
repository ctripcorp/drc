package com.ctrip.framework.drc.fetcher.resource.transformer;

import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by jixinwang on 2022/1/12
 */
public class TransformerContextResource extends AbstractResource implements TransformerContext {

    @InstanceConfig(path = "nameMapping")
    public String nameMapping;

    private Map<String, String> nameMap = Maps.newLinkedHashMap();

    public Map<String, String> getNameMap() {
        return nameMap;
    }

    @VisibleForTesting
    protected void setNameMapping(String nameMapping) {
        this.nameMapping = nameMapping;
    }

    @Override
    public void doInitialize() throws Exception {
        parseNameMapping();
    }

    @VisibleForTesting
    protected void parseNameMapping() {
        logger.info("name mapping is: {}", nameMapping);
        if (StringUtils.isBlank(nameMapping)) {
            return;
        }
        nameMapping = nameMapping.trim().toLowerCase();
        String[] names = StringUtils.split(nameMapping, ';');

        for (String name : names) {
            String[] namePair = StringUtils.split(name, ',');
            String sourceName = namePair[0];
            String targetName = namePair[1];

            if (sourceName.contains("[")) {
                List<String> sourceNames = parseNameMode(sourceName);
                List<String> targetNames = parseNameMode(targetName);
                if (sourceNames.size() != targetNames.size()) {
                    logger.error("source name: {} and target name: {} size are not same", sourceNames, targetNames);
                    throw new RuntimeException("source and target name size are not same");
                }
                for (int i = 0; i < sourceNames.size(); i++) {
                    nameMap.put(sourceNames.get(i), targetNames.get(i));
                }
            } else {
                nameMap.put(parseSingleName(sourceName), parseSingleName(targetName));
            }
        }
        logger.info("init name mapping success, size is: {}, content is: {}", nameMap.size(), nameMap);
    }

    private List<String> parseNameMode(String name) {
        String[] names = StringUtils.split(name, '.');
        String schemaMode = names[0];
        String tableMode = names[1];

        List<String> schemas = TransformerHelper.parseMode(schemaMode);
        List<String> tables = TransformerHelper.parseMode(tableMode);

        List<String> schemaDotTables = Lists.newArrayList();
        for (String schema : schemas) {
            for (String table : tables) {
                schemaDotTables.add("`" + schema + "`.`" + table + "`");
            }
        }
        return schemaDotTables;
    }

    private String parseSingleName(String name) {
        String[] names = StringUtils.split(name, '.');
        String schema = names[0];
        String table = names[1];
        return "`" + schema + "`.`" + table + "`";
    }
}
