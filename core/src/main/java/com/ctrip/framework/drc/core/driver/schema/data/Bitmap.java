package com.ctrip.framework.drc.core.driver.schema.data;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author Slight
 * Jul 02, 2020
 */
public class Bitmap extends ArrayList<Boolean> implements List<Boolean> {

    public static Bitmap from(Boolean... bits) {
        Bitmap bitmap = new Bitmap();
        Collections.addAll(bitmap, bits);
        return bitmap;
    }

    public static Bitmap from(List<Boolean> another) {
        Bitmap bitmap = new Bitmap();
        bitmap.addAll(another);
        return bitmap;
    }

    public static <T> Bitmap from(List<T> sub, List<T> origin) {
        Boolean[] bits = new Boolean[origin.size()];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = sub.contains(origin.get(i));
        }
        return from(bits);
    }

    public static Bitmap fromMarks(Integer... marks) {
        return fromMarks(Lists.newArrayList(marks));
    }

    //eg.
    //[2, 1] -> [false, true, true]
    //[3] -> [false, false, false, true]
    //[0, 3] -> [true, false, false, true]
    //[0, 1] -> [true, true]
    public static Bitmap fromMarks(List<Integer> marks) {
        Collections.sort(marks);
        Bitmap bitmap = new Bitmap();
        int size = marks.size();
        int max = marks.get(size - 1);
        for (int i = 0; i <= max; i++) {
            bitmap.add(i, marks.contains(i));
        }
        return bitmap;
    }

    public static Bitmap union(List<Boolean> a, List<Boolean> b) {
        Bitmap bitmap = new Bitmap();
        List<Boolean> bigger = a;
        List<Boolean> smaller = b;
        if (a.size() < b.size()) {
            bigger = b;
            smaller = a;
        }

        bitmap.addAll(bigger);
        for (int i = 0; i < smaller.size(); i++) {
            if (smaller.get(i)) {
                bitmap.set(i, true);
            }
        }
        return bitmap;
    }

    public boolean isSubsetOf(List<Boolean> another) {
        for (int i = 0; i < size(); i++) {
            if (get(i)) {
                if (i >= another.size()) {
                    return false;
                }
                if (!another.get(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    public <T> List<T> on(List<T> origin) {
        List<T> sublist = Lists.newArrayList();
        for (int i = 0; i < Math.min(origin.size(), size()); i++) {
            if (get(i)) {
                sublist.add(origin.get(i));
            }
        }
        return sublist;
    }

    public <T> List<T> under(List<T> origin) {
        List<T> sublist = Lists.newArrayList();
        for (int i = 0; i < origin.size(); i++) {
            if (i < size() && get(i)) continue;
            sublist.add(origin.get(i));
        }
        return sublist;
    }

    public Bitmap onBitmap(Bitmap another) {
        return Bitmap.from(on(another));
    }

    public Bitmap underBitmap(Bitmap another) {
        return Bitmap.from(under(another));
    }

    @Override
    public Object clone() {
        List<Boolean> clone = this.stream().collect(Collectors.toList());
        return Bitmap.from(clone);
    }
}
