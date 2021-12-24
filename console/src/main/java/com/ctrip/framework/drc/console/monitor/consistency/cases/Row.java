package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.google.common.collect.Lists;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Objects;

/**
 * Created by mingdongli
 * 2019/11/20 下午8:06.
 */
public class Row {

    private List<Object> fields = Lists.newArrayList();

    public List<Object> getFields() {
        return fields;
    }

    public void addField(Object field) {
        fields.add(field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        List<Object> fieldsList = row.getFields();

        int srcColumnCount = fields.size();
        int dstColumnCount = fieldsList.size();
        if (srcColumnCount != dstColumnCount) {
            return false;
        }
        for (int i = 0; i < srcColumnCount; ++i) {
            Object srcObject = fields.get(i);
            Object dstObject = fieldsList.get(i);
            if (srcObject == null && dstObject == null) {
                continue;
            }

            if ((srcObject == null && dstObject != null) || (srcObject != null && dstObject == null)) {
                return false;
            }

            assert srcObject != null;
            assert dstObject != null;

            if (srcObject.getClass().isArray() && dstObject.getClass().isArray()) {
                if (Array.getLength(srcObject) != Array.getLength(dstObject)) {
                    return false;
                }
                for(int j = 0; j < Array.getLength(srcObject); j++){
                    if (!Array.get(srcObject, j).equals(Array.get(dstObject, j))) {
                        return false;
                    }
                }
                continue;
            }

            if (!srcObject.equals(dstObject)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {

        return Objects.hash(fields);
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();
        for (Object o : fields) {
            stringBuilder.append(o.toString()).append(",");
        }
        return "Row{" +
                "fields=" + stringBuilder.toString() +
                '}';
    }
}
