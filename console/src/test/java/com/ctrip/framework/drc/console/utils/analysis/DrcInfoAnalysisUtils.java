package com.ctrip.framework.drc.console.utils.analysis;


import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName DrcInfoAnalysisUtils
 * @Author haodongPan
 * @Date 2023/6/15 16:02
 * @Version: $
 */
public class DrcInfoAnalysisUtils {

    private static final Gson gson = new Gson();

    @Test
    public void test() throws IOException {
        // get json from api :/api/drc/v1/openapi/info/dbs
        URL resource = this.getClass().getClassLoader().getResource("analysis/drc.json");
        List<DrcDbInfo> dbInfos = readFromFile(resource.getPath());
//        countRegion(dbInfos,"sha","sin");
//        countRegion(dbInfos,"sha","fra");
//        countVpc(dbInfos);
        countAll(dbInfos);
    }

    private void countVpc(List<DrcDbInfo> dbInfos) throws IOException {
        List<DrcDbInfo> sha2sinibuaws = filterByRegion(dbInfos, "sha", "sinibuaws");
        List<DrcDbInfo> sha2ali = filterByRegion(dbInfos, "sha", "sinibualiyun");
        List<DrcDbInfo> aws2sha = filterByRegion(dbInfos, "sinibuaws", "sha");
        List<DrcDbInfo> ali2sha = filterByRegion(dbInfos, "sinibualiyun", "sha");

        List<DrcDbInfo> vpc = Lists.newArrayList();
        vpc.addAll(sha2sinibuaws);
        vpc.addAll(sha2ali);
        vpc.addAll(aws2sha);
        vpc.addAll(ali2sha);
        
        List<DrcDbInfo> rowsFilterDbInfos = filterByRowsFilter(vpc);
        List<DrcDbInfo> columnsFilterDbInfos = filterByColumnsFilter(vpc);
        String res = "vpc" + "\n" + getAllDistinctDbs(vpc);
        res += "\n\nrowsFilterDbInfos:\n" + getAllDistinctDbs(rowsFilterDbInfos);
        res += "\n\ncolumnsFilterDbInfos:\n" + getAllDistinctDbs(columnsFilterDbInfos);

        URL resource = this.getClass().getClassLoader().getResource("analysis/drc.json");
        File file = new File( resource.getPath() +"/../vpc"  + ".txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream fileOs = new FileOutputStream(file);
        byte[] bytes = res.getBytes();
        fileOs.write(bytes);
        fileOs.flush();
        fileOs.close();
    }
    
    
    private void countRegion(List<DrcDbInfo> dbInfos,String srcRegion,String destRegion) throws IOException {
        List<DrcDbInfo> sha2sin = filterByRegion(dbInfos, srcRegion, destRegion);
        List<DrcDbInfo> rowsFilterDbInfos = filterByRowsFilter(sha2sin);
        List<DrcDbInfo> columnsFilterDbInfos = filterByColumnsFilter(sha2sin);
        String res = srcRegion + "2" + destRegion + "\n" + getAllDistinctDbs(sha2sin);
        res += "\n\nrowsFilterDbInfos:\n" + getAllDistinctDbs(rowsFilterDbInfos);
        res += "\n\ncolumnsFilterDbInfos:\n" + getAllDistinctDbs(columnsFilterDbInfos);

        URL resource = this.getClass().getClassLoader().getResource("analysis/drc.json");
        File file = new File( resource.getPath() +"/../" + srcRegion + "2" + destRegion + ".txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream fileOs = new FileOutputStream(file);
        byte[] bytes = res.getBytes();
        fileOs.write(bytes);
        fileOs.flush();
        fileOs.close();
    }
    
    
    private void countAll(List<DrcDbInfo> dbInfos) throws IOException {
        List<DrcDbInfo> sha = filterBySrcRegion(dbInfos,"sha");
        List<DrcDbInfo> sin = filterBySrcRegion(dbInfos,"sin");
        List<DrcDbInfo> fra = filterBySrcRegion(dbInfos,"fra");
        List<DrcDbInfo> sinibuaws = filterBySrcRegion(dbInfos,"sinibuaws");
        List<DrcDbInfo> all = Lists.newArrayList();
        all.addAll(sha);
        all.addAll(sin);
        all.addAll(fra);
        all.addAll(sinibuaws);
        List<DrcDbInfo> rowsFilterDbInfos = filterByRowsFilter(all);
        List<DrcDbInfo> columnsFilterDbInfos = filterByColumnsFilter(all);
        String res = "sha:\n" + getAllDistinctDbs(all);
        res += "\n\nrowsFilterDbInfos:\n" + getAllDistinctDbs(rowsFilterDbInfos);
        res += "\n\ncolumnsFilterDbInfos:\n" + getAllDistinctDbs(columnsFilterDbInfos);


        URL resource = this.getClass().getClassLoader().getResource("analysis/drc.json");
        
        File file = new File(resource.getPath() + "/../" + "allDrcCount.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream fileOs = new FileOutputStream(file);
        byte[] bytes = res.getBytes();
        fileOs.write(bytes);
        fileOs.flush();
        fileOs.close();
    }
    
    
    private List<DrcDbInfo> readFromFile (String filePath) throws IOException {
        String input = FileUtils.readFileToString(new File(filePath));
        JsonObject jsonObject = gson.fromJson(input, JsonObject.class);
        JsonArray dbInfosJson = jsonObject.get("data").getAsJsonArray();
        List<DrcDbInfo> dbInfos = Lists.newArrayList();
        for (JsonElement element : dbInfosJson) {
            DrcDbInfo drcDbInfo = gson.fromJson(element, DrcDbInfo.class);
            dbInfos.add(drcDbInfo);
        }
        return dbInfos;
    }

    private List<DrcDbInfo> filterBySrcRegion(List<DrcDbInfo> dbInfos,String srcRegion) {
        List<DrcDbInfo> res = Lists.newArrayList();
        for (DrcDbInfo dbInfo : dbInfos) {
            if (srcRegion.equalsIgnoreCase(dbInfo.getSrcRegion())) {
                res.add(dbInfo);
            }
        }
        return res;
    }
    
    private List<DrcDbInfo> filterByRegion(List<DrcDbInfo> dbInfos,String srcRegion,String destRegion) {
        List<DrcDbInfo> res = Lists.newArrayList();
        for (DrcDbInfo dbInfo : dbInfos) {
            if (srcRegion.equalsIgnoreCase(dbInfo.getSrcRegion()) && destRegion.equalsIgnoreCase(dbInfo.getDestRegion())) {
                res.add(dbInfo);
            }
        }
        return res;
    }

    private List<DrcDbInfo> filterByRowsFilter(List<DrcDbInfo> dbInfos) {
        List<DrcDbInfo> res = Lists.newArrayList();
        for (DrcDbInfo dbInfo : dbInfos) {
            if (!CollectionUtils.isEmpty(dbInfo.getRowsFilterConfigs())) {
                res.add(dbInfo);
            }
        }
        return res;
    }
    
    private List<DrcDbInfo> filterByColumnsFilter(List<DrcDbInfo> dbInfos) {
        List<DrcDbInfo> res = Lists.newArrayList();
        for (DrcDbInfo dbInfo : dbInfos) {
            if (!CollectionUtils.isEmpty(dbInfo.getColumnsFilterConfigs())) {
                res.add(dbInfo);
            }
        }
        return res;
    }
    
    private String getAllDistinctDbs(List<DrcDbInfo> dbInfos) {
        HashSet<String> dbNames = Sets.newHashSet();
        HashSet<String> shardDbsPrefix = Sets.newHashSet();
        List<String> dbs = Lists.newArrayList();
        StringBuilder res = new StringBuilder();
        for (DrcDbInfo dbInfo : dbInfos) {
            String db = dbInfo.getDb();
            if (!dbNames.contains(db)) {
                dbs.add(db);
                dbNames.add(db);
            }
        }
        System.out.println("disctinct dbs: " + dbs.size());
        
        // sort & move shardDbs to head
        dbs.sort(String::compareToIgnoreCase);
        List<String> dbsSorted = Lists.newArrayList();
        for (String db : dbs) {
            if (db.contains("shard")) {
                int shard = db.indexOf("shard");
                shardDbsPrefix.add(db.substring(0,shard));
                dbsSorted.add(0,db);
            } else if (db.contains("Shard")) {
                int shard = db.indexOf("Shard");
                shardDbsPrefix.add(db.substring(0,shard));
                dbsSorted.add(0,db);
            } else {
                dbsSorted.add(db);
            }
        }
        
        
        // put the same ShardDbs in one line
        String currentPrefix = null;
        for (String db : dbsSorted) {
            if (db.contains("shard")) {
                int shard = db.indexOf("shard");
                String prefix = db.substring(0,shard);
                if (prefix.equalsIgnoreCase(currentPrefix)) {
                    res.append(",").append(db);
                } else {
                    res.append("\n").append(db);
                }
                currentPrefix = prefix;
            } else if (db.contains("Shard")) {
                int shard = db.indexOf("Shard");
                String prefix = db.substring(0,shard);
                if (prefix.equalsIgnoreCase(currentPrefix)) {
                    res.append(",").append(db);
                } else {
                    res.append("\n").append(db);
                }
                currentPrefix = prefix;
            } else {
                res.append("\n").append(db);
            }
        }

        System.out.println("dalCluster size:" + res.toString().split("\n").length);
        
        return res.length() > 1 ? res.substring(1, res.length()) : res.toString();
        
    }
    
}
