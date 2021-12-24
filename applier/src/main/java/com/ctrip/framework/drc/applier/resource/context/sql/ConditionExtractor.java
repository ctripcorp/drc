package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @Author Slight
 * Dec 10, 2019
 */
public class ConditionExtractor implements ColumnsContainer {

    private final Columns columns;
    private final List<List<Object>> rowsOfValues;
    private final Bitmap bitmapOfValues;
    private final List<List<Object>> rowsOfIdentifier;
    private final List<Object> rowsOfColumnOnUpdate;
    private final List<List<Object>> rowsOfConditions;
    private boolean calculated = false;

    private Bitmap bitmapOfIdentifier;
    private Bitmap bitmapOfColumnOnUpdate;
    private Bitmap bitmapOfConditions;

    public ConditionExtractor(Columns columns, List<List<Object>> rowsOfValues, List<Boolean> bitmapOfValues) {
        this.columns = columns;
        //values
        this.rowsOfValues = rowsOfValues;
        this.bitmapOfValues = Bitmap.from(bitmapOfValues);
        //identifier
        this.rowsOfIdentifier = Lists.newArrayList();
        //column on update
        this.rowsOfColumnOnUpdate = Lists.newArrayList();
        //conditions
        this.rowsOfConditions = Lists.newArrayList();
    }

    public List<List<Object>> getRowsOfValues() {
        return rowsOfValues;
    }

    public List<Boolean> getBitmapOfValues() {
        return bitmapOfValues;
    }

    public List<List<Object>> getRowsOfConditions() {
        calculateIf();
        return rowsOfConditions;
    }

    public List<Boolean> getBitmapOfConditions() {
        calculateIf();
        return bitmapOfConditions;
    }

    public List<List<Object>> getRowsOfIdentifier() {
        calculateIf();
        return rowsOfIdentifier;
    }

    public List<Boolean> getBitmapOfIdentifier() {
        return bitmapOfIdentifier;
    }

    public List<Object> getRowsOfColumnOnUpdate() {
        calculateIf();
        return rowsOfColumnOnUpdate;
    }

    public List<Boolean> getBitmapOfColumnOnUpdate() {
        return bitmapOfColumnOnUpdate;
    }

    private void calculateIf() {
        if (calculated) {
            return;
        }
        calculated = true;
        //solution on the way
        //assert rowsOfValues.get(0).size() <= columns.size();
        //assert bitmapOfValues.size() <= columns.size();

        //do invalidation for columns elsewhere
        //assert columns.getIdentifiers().size() > 0;
        //assert columns.getColumnsOnUpdate().size() > 0;
        Bitmap identifier = getIdentifier(bitmapOfValues);
        Bitmap columnOnUpdate = getColumnOnUpdate(bitmapOfValues);
        assert identifier != null : "no valid identifier found for:\n"
                + rowsOfValues.get(0) + "\n" + bitmapOfValues + "\n" + columns.getNames();
        assert columnOnUpdate != null : "no valid column on update found for:\n"
                + rowsOfValues.get(0) + "\n" + bitmapOfValues + "\n" + columns.getNames();
        bitmapOfIdentifier = identifier;
        bitmapOfColumnOnUpdate = columnOnUpdate;
        bitmapOfConditions = Bitmap.union(identifier, columnOnUpdate);
        for (List<Object> row : rowsOfValues) {
            rowsOfIdentifier.add(
                    Bitmap.from(
                            bitmapOfValues.on(bitmapOfIdentifier)
                    ).on(row));

            rowsOfColumnOnUpdate.add(
                    Bitmap.from(
                            bitmapOfValues.on(bitmapOfColumnOnUpdate)
                    ).on(row).get(0));

            rowsOfConditions.add(
                    Bitmap.from(
                            bitmapOfValues.on(bitmapOfConditions)
                    ).on(row));
        }
    }

    @Override
    public Columns getColumns() {
        return columns;
    }
}
