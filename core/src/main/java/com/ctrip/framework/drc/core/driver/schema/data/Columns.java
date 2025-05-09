package com.ctrip.framework.drc.core.driver.schema.data;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent.Column;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @Author Slight
 * Aug 12, 2020
 */
public class Columns extends ArrayList<Column> {

    private static final String STANDARD_ON_UPDATE_COLUMN_NAME = "datachange_lasttime";
    private List<List<String>> identifiers;

    private boolean arranged = false;
    private List<Bitmap> bitmapsOfIdentifier;
    private List<Bitmap> bitmapsOnUpdate;
    private Bitmap standardOnUpdate;

    private void arrangeOnce() {
        if (!arranged) {
            arrange();
            arranged = true;
        }
    }

    public List<Bitmap> getBitmapsOfIdentifier() {
        arrangeOnce();
        return bitmapsOfIdentifier;
    }

    public List<Bitmap> getBitmapsOnUpdate() {
        arrangeOnce();
        return bitmapsOnUpdate;
    }

    public Bitmap getLastBitmapOnUpdate() {
        List<Bitmap> bitmaps = getBitmapsOnUpdate();
        return (standardOnUpdate == null ? bitmaps.get(bitmaps.size() - 1) : standardOnUpdate);
    }

    public static Columns from(List<Column> columns) {
        return new Columns(columns, Lists.newArrayList());
    }

    public static Columns from(List<Column> columns, List<List<String>> identifiers) {
        return new Columns(columns, identifiers);
    }

    public Columns() {
    }

    public Columns(List<Column> columns, List<List<String>> identifiers) {
        addAll(columns);
        this.identifiers = identifiers;
        this.arranged = false;
    }

    private void arrange() {
        bitmapsOfIdentifier = Lists.newArrayList();
        bitmapsOnUpdate = Lists.newArrayList();

        List<Integer> pks = Lists.newArrayList();
        for (int i = 0; i < size(); i++) {
            Column column = get(i);
            if (column.isPk()) {
                pks.add(i);

            }
            if (column.isOnUpdate()) {
                if (STANDARD_ON_UPDATE_COLUMN_NAME.equalsIgnoreCase(column.getName())) {
                    standardOnUpdate = Bitmap.fromMarks(i);
                }
                bitmapsOnUpdate.add(Bitmap.fromMarks(i));
            }
        }

        if (!pks.isEmpty()) {
            bitmapsOfIdentifier.add(Bitmap.fromMarks(pks));
        }

        List<String> columnNames = getNames();
        for (int i = 1; i < identifiers.size(); i++) {
            List<String> identifier = identifiers.get(i);
            bitmapsOfIdentifier.add(Bitmap.from(identifier, columnNames));
        }
    }

    public List<String> getNames() {
        return this.stream()
                .map(Column::getName)
                .collect(toList());
    }

    public List<Object> getColumnDefaults() {
        return this.stream()
                .map(Column::getColumnDefaultObject)
                .collect(toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Columns)) return false;
        if (!super.equals(o)) return false;
        Columns columns = (Columns) o;
        return Objects.equals(identifiers, columns.identifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), identifiers);
    }

    public void removeColumn(String name) {
        this.removeIf(column -> name.equals(column.getName()));
        identifiers.removeIf(keyCombination -> keyCombination.contains(name));
        arranged = false;
    }

    @Override
    public Object clone() {
        List<Column> columnClone = this.stream().collect(Collectors.toList());
        List<List<String>> identifiersClone = this.identifiers.stream().collect(Collectors.toList());
        Columns cloneColumns = Columns.from(columnClone, identifiersClone);

        if (this.bitmapsOfIdentifier != null) {
            List<Bitmap> bitmapsOfIdentifierClone = this.bitmapsOfIdentifier.stream().collect(Collectors.toList());
            cloneColumns.bitmapsOfIdentifier = bitmapsOfIdentifierClone;
        }
        if (this.bitmapsOnUpdate != null) {
            List<Bitmap> bitmapsOnUpdateClone = this.bitmapsOnUpdate.stream().collect(Collectors.toList());
            cloneColumns.bitmapsOnUpdate = bitmapsOnUpdateClone;
        }

        cloneColumns.arranged = this.arranged;
        cloneColumns.standardOnUpdate = this.standardOnUpdate;
        return cloneColumns;
    }
}
