package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;

import java.util.List;

/**
 * @Author Slight
 * Dec 10, 2019
 */
public interface ColumnsContainer {

    //example 0:
    //
    //condition map: [false, true, true, false, true]
    //pk: [0]
    //index: [1]
    //joint index: [3,4]
    //
    //In this case:
    //  [1] will be matched.
    // That is, the second column (which is unique key) is selected to the identifier.
    //
    //
    //example 1:
    //
    //condition map: [true, true, true, false, true]
    // pk: [0]
    // index: [1]
    //In this case:
    //  [0] will be matched.
    // That is, the first column (which is the primary key) is selected to the identifier.
    //
    default Bitmap getIdentifier(List<Boolean> conditionMap) {
        for (Bitmap identifier : getColumns().getBitmapsOfIdentifier()) {
            if (identifier.isSubsetOf(conditionMap)) {
                return identifier;
            }
        }
        return null;
    }

    default Bitmap getColumnOnUpdate(List<Boolean> conditionMap) {
        Bitmap result = null;
        for (Bitmap onUpdate : getColumns().getBitmapsOnUpdate()) {
            if (onUpdate.isSubsetOf(conditionMap))
                result = onUpdate;
        }
        return result;
    }

    Columns getColumns();
}
