package com.ctrip.framework.drc.manager.controller.applier;

import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDeltaDto;
import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDto;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class SchemasHistoryComponent {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ZkClient zkClient;

    @PostConstruct
    public void init() {
        try {
            if (zkClient.get().checkExists().forPath("/applier") == null){
                // 递归创建节点
                zkClient.get().create().creatingParentsIfNeeded().forPath("/applier");
            }
        } catch (Exception e) {
            logger.error("schema history component init fail", e);
        }
    }

    class CompoundNode {
        private String base;
        private int index = -1;
        private SchemasHistoryDto history;
        CompoundNode(String path) {
            this.base = path;
        }

        private void initIndexIf() throws Exception {
            CuratorFramework client = zkClient.get();
            if (index < 0) {
                if (client.checkExists().forPath(base) == null) {
                    client.create().creatingParentsIfNeeded().forPath(base, "0".getBytes());
                    index = 0;
                    return;
                } else {
                    index = Integer.parseInt(new String(client.getData().forPath(base)));
                }
            }
            return;
        }

        private void updateIndex() throws Exception {
            CuratorFramework client = zkClient.get();
            client.setData().forPath(base, (index + "").getBytes());
        }

        private void createNodeIf() throws Exception {
            CuratorFramework client = zkClient.get();
            String path = base + "-" + index;
            if (client.checkExists().forPath(path) == null) {
                SchemasHistoryDto history = new SchemasHistoryDto();
                history.setSchemasHistoryDeltas(Lists.newArrayList());
                client.create().creatingParentsIfNeeded().forPath(path, Codec.DEFAULT.encodeAsBytes(history));
            }
        }

        public void add(SchemasHistoryDeltaDto delta) throws Exception {
            CuratorFramework client = zkClient.get();
            initIndexIf();
            createNodeIf();
            byte[] historyBytes = client.getData().forPath(base + "-" + index);
            if (historyBytes.length > 990000) {
                index = index + 1;
                createNodeIf();
                updateIndex();
                historyBytes = client.getData().forPath(base + "-" + index);
            }
            history = Codec.DEFAULT.decode(historyBytes, SchemasHistoryDto.class);
            history.add(delta);
            historyBytes = Codec.DEFAULT.encodeAsBytes(history);
            if (historyBytes.length > 1000000) {
                throw new Exception("size of schemas history exceeds limit.");
            }
            client.setData().forPath(base + "-" + index, historyBytes);
        }

        public SchemasHistoryDto getAll() throws Exception {
            CuratorFramework client = zkClient.get();
            initIndexIf();
            createNodeIf();
            SchemasHistoryDto result = SchemasHistoryDto.create();
            byte[] temp;
            SchemasHistoryDto t;
            for (int i = 0; i < index + 1; i++) {
                temp = client.getData().forPath(base + "-" + i);
                result.merge(Codec.DEFAULT.decode(temp, SchemasHistoryDto.class));
            }
            logger.info("size of history:" + result.getSchemasHistoryDeltas().size());
            return result;
        }
    }

    private String path(String applierId) {
        return "/applier/schemas/" + applierId;
    }

    public void merge(String applierId, SchemasHistoryDeltaDto delta) throws Exception {
        String path = path(applierId);
        new CompoundNode(path).add(delta);
    }

    public SchemasHistoryDto fetch(String applierId) throws Exception {
        String path = path(applierId);
        return new CompoundNode(path).getAll();
    }
}
