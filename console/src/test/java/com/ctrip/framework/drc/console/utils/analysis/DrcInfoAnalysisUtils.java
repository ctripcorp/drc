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
        countRegion(dbInfos,"sha","sin");
        countRegion(dbInfos,"sha","fra");
        countRegion(dbInfos,"sha","sinibuaws");
        countAll(dbInfos);
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
        List<DrcDbInfo> rowsFilterDbInfos = filterByRowsFilter(sha);
        List<DrcDbInfo> columnsFilterDbInfos = filterByColumnsFilter(sha);
        String res = "sha:\n" + getAllDistinctDbs(sha);
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
        
        return res.length() > 1 ? res.substring(1, res.length()) : res.toString();
        
    }
    
}
