package com.ctrip.framework.drc.console.dto;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Created by jixinwang on 2021/2/23
 */
public class CurrentRecordPairDto {
    private List<Map<String, Object>> mhaAColumnPattern;
    private List<Map<String, Object>> mhaBColumnPattern;
    private List<String> mhaAColumnNameList;
    private List<String> mhaBColumnNameList;
    private Map<String, CurrentResultSetPairDto> keyAndCurrentResultSetPairMap;
    private List<Map<String, Object>> mhaACurrentResultList;
    private List<Map<String, Object>> mhaBCurrentResultList;
    private int differentCount;
    private boolean isDdlDifferent;

    public List<Map<String, Object>> getMhaAColumnPattern() {
        return mhaAColumnPattern;
    }

    public void setMhaAColumnPattern(List<Map<String, Object>> mhaAColumnPattern) {
        this.mhaAColumnPattern = mhaAColumnPattern;
    }

    public List<Map<String, Object>> getMhaBColumnPattern() {
        return mhaBColumnPattern;
    }

    public void setMhaBColumnPattern(List<Map<String, Object>> mhaBColumnPattern) {
        this.mhaBColumnPattern = mhaBColumnPattern;
    }

    public List<String> getMhaAColumnNameList() {
        return mhaAColumnNameList;
    }

    public void setMhaAColumnNameList(List<String> mhaAColumnNameList) {
        this.mhaAColumnNameList = mhaAColumnNameList;
    }

    public List<String> getMhaBColumnNameList() {
        return mhaBColumnNameList;
    }

    public void setMhaBColumnNameList(List<String> mhaBColumnNameList) {
        this.mhaBColumnNameList = mhaBColumnNameList;
    }

    public Map<String, CurrentResultSetPairDto> getKeyAndCurrentResultSetPairMap() {
        return keyAndCurrentResultSetPairMap;
    }

    public void setKeyAndCurrentResultSetPairMap(Map<String, CurrentResultSetPairDto> keyAndCurrentResultSetPairMap) {
        this.keyAndCurrentResultSetPairMap = keyAndCurrentResultSetPairMap;
    }

    public List<Map<String, Object>> getMhaACurrentResultList() {
        return mhaACurrentResultList;
    }

    public void setMhaACurrentResultList(List<Map<String, Object>> mhaACurrentResultList) {
        this.mhaACurrentResultList = mhaACurrentResultList;
    }

    public List<Map<String, Object>> getMhaBCurrentResultList() {
        return mhaBCurrentResultList;
    }

    public void setMhaBCurrentResultList(List<Map<String, Object>> mhaBCurrentResultList) {
        this.mhaBCurrentResultList = mhaBCurrentResultList;
    }

    public int getDifferentCount() {
        return differentCount;
    }

    public void setDifferentCount(int differentCount) {
        this.differentCount = differentCount;
    }

    public boolean getIsDdlDifferent() {
        return isDdlDifferent;
    }

    public void setDdlDifferent(boolean ddlDifferent) {
        this.isDdlDifferent = ddlDifferent;
    }

    public void setCurrentResultList() {
        List<Map<String, Object>> mhaACurrentResultList = new ArrayList<>();
        List<Map<String, Object>> mhaBCurrentResultList = new ArrayList<>();
        for(CurrentResultSetPairDto entry : keyAndCurrentResultSetPairMap.values()) {
            Map<String, Object> mhaACurrentResult = entry.getMhaACurrentResult();
            if (mhaACurrentResult != null && !mhaACurrentResult.isEmpty()) {
                mhaACurrentResultList.add(mhaACurrentResult);
            }
            Map<String, Object> mhaBCurrentResult = entry.getMhaBCurrentResult();
            if (mhaBCurrentResult != null && !mhaBCurrentResult.isEmpty()) {
                mhaBCurrentResultList.add(mhaBCurrentResult);
            }
        }
        setMhaACurrentResultList(mhaACurrentResultList);
        setMhaBCurrentResultList(mhaBCurrentResultList);
    }

    public void setKeyAndCurrentResultSetPairMap(CurrentRecordDto mhaACurrentRecordDto, CurrentRecordDto mhaBCurrentRecordDto) {
        //union keyValueList of mha and mhaB
        List<String> mhaAKeyValueList = mhaACurrentRecordDto.getKeyValueList();
        List<String> mhaBKeyValueList = mhaACurrentRecordDto.getKeyValueList();
        Set<String> keyValueSet = new HashSet<String>(mhaAKeyValueList);
        keyValueSet.addAll(mhaBKeyValueList);
        Map<String, CurrentResultSetPairDto> keyAndCurrentResultSetPairMap = new HashMap<>();
        for (String keyValue : keyValueSet) {
            CurrentResultSetPairDto currentResultSetPairDto = new CurrentResultSetPairDto();
            keyAndCurrentResultSetPairMap.put(keyValue, currentResultSetPairDto);
        }
        for (CurrentResultSetDto currentResultSetDto : mhaACurrentRecordDto.getCurrentResultSetDto()) {
            String keyValue = currentResultSetDto.getKeyValue();
            CurrentResultSetPairDto currentResultSetPairDto = keyAndCurrentResultSetPairMap.get(keyValue);
            currentResultSetPairDto.setMhaACurrentResult(currentResultSetDto.getCurrentResult());
        }
        for (CurrentResultSetDto currentResultSetDto : mhaBCurrentRecordDto.getCurrentResultSetDto()) {
            String keyValue = currentResultSetDto.getKeyValue();
            CurrentResultSetPairDto currentResultSetPairDto = keyAndCurrentResultSetPairMap.get(keyValue);
            currentResultSetPairDto.setMhaBCurrentResult(currentResultSetDto.getCurrentResult());
        }
        setKeyAndCurrentResultSetPairMap(keyAndCurrentResultSetPairMap);
    }

    public boolean markDifferentRecord() {
        List<String> mhaAExtraColumnList = mhaAColumnNameList.stream().filter(item -> !mhaBColumnNameList.contains(item)).collect(toList());
        List<String> mhaBExtraColumnList = mhaBColumnNameList.stream().filter(item -> !mhaAColumnNameList.contains(item)).collect(toList());
        boolean markExtraColumn = markExtraColumn(keyAndCurrentResultSetPairMap, mhaAExtraColumnList, mhaBExtraColumnList);

        //the intersection of src and dest
        List<String> intersectionList = mhaAColumnNameList.stream().filter(item -> mhaBColumnNameList.contains(item)).collect(toList());
        boolean markDifferentColumn = markDifferentColumn(keyAndCurrentResultSetPairMap, intersectionList);

        return markExtraColumn || markDifferentColumn;
    }

    public boolean markExtraColumn(Map<String, CurrentResultSetPairDto> keyAndCurrentResultSetPairMap, List<String> mhaAExtraColumnList, List<String> mhaBExtraColumnList) {
        if (mhaAExtraColumnList.isEmpty() && mhaBExtraColumnList.isEmpty()) {
            setDdlDifferent(false);
            return false;
        }
        for(CurrentResultSetPairDto entry : keyAndCurrentResultSetPairMap.values()) {
            Map<String, Object> mhaACurrentResult = entry.getMhaACurrentResult();
            if (mhaACurrentResult != null) {
                Map<String, String> mhaACellClassName = (Map<String, String>) mhaACurrentResult.get("cellClassName");
                for (String extraColumn : mhaAExtraColumnList) {
                    mhaACellClassName.put(extraColumn, "table-info-cell-extra-column-add");
                }
            }
            Map<String, Object> mhaBCurrentResult = entry.getMhaBCurrentResult();
            if (mhaBCurrentResult != null) {
                Map<String, String> mhaBCellClassName = (Map<String, String>) mhaBCurrentResult.get("cellClassName");
                for (String extraColumn : mhaBExtraColumnList) {
                    mhaBCellClassName.put(extraColumn, "table-info-cell-extra-column-add");
                }
            }
        }
        setDdlDifferent(true);
        return true;
    }

    public boolean markDifferentColumn(Map<String, CurrentResultSetPairDto> keyAndCurrentResultSetPairMap, List<String> intersectionList) {
        int differentCount = 0;
        boolean markDifferentColumn = false;
        for(CurrentResultSetPairDto entry : keyAndCurrentResultSetPairMap.values()) {
            Map<String, Object> mhaACurrentResult = entry.getMhaACurrentResult();
            Map<String, Object> mhaBCurrentResult = entry.getMhaBCurrentResult();
            if (mhaACurrentResult == null && mhaBCurrentResult == null) {
                continue;
            }
            if (mhaACurrentResult == null) {
                markAllColumnDiff(mhaBCurrentResult, intersectionList);
                markDifferentColumn = true;
                differentCount++;
                continue;
            }
            if (mhaBCurrentResult == null) {
                markAllColumnDiff(mhaACurrentResult, intersectionList);
                markDifferentColumn = true;
                differentCount++;
                continue;
            }
            List<String> diffColumnList = new ArrayList<>();
            for (String columnName : intersectionList) {
                Object srcRecordValue = mhaACurrentResult.get(columnName);
                Object destRecordValue = mhaBCurrentResult.get(columnName);
                if (srcRecordValue == null && destRecordValue == null) {
                    continue;
                }
                if (srcRecordValue == null) {
                    diffColumnList.add(columnName);
                    differentCount++;
                    continue;
                }
                if (destRecordValue == null) {
                    diffColumnList.add(columnName);
                    differentCount++;
                    continue;
                }
                if(!srcRecordValue.equals(destRecordValue)) {
                    diffColumnList.add(columnName);
                    differentCount++;
                }
            }
            Map<String, String> srcCellClassName = (Map<String, String>) mhaACurrentResult.get("cellClassName");
            Map<String, String> destCellClassName = (Map<String, String>) mhaBCurrentResult.get("cellClassName");
            for (String diffColumn : diffColumnList) {
                srcCellClassName.put(diffColumn, "table-info-cell-extra-column-diff");
                destCellClassName.put(diffColumn, "table-info-cell-extra-column-diff");
            }
            if (!diffColumnList.isEmpty()) {
                markDifferentColumn = true;
            }
        }
        if (getIsDdlDifferent()) {
            setDifferentCount(keyAndCurrentResultSetPairMap.size());
        } else {
            setDifferentCount(differentCount);
        }
        return markDifferentColumn;
    }

    public void markAllColumnDiff(Map<String, Object> currentResult, List<String> intersectionList) {
        Map<String, String> cellClassName = (Map<String, String>) currentResult.get("cellClassName");
        for (String diffColumn : intersectionList) {
            cellClassName.put(diffColumn, "table-info-cell-extra-column-diff");
        }
    }
}
