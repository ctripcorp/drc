package com.ctrip.framework.drc.core.filter.aviator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.ctrip.framework.drc.core.filter.exception.FilterException;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

/**
 * Created by jixinwang on 2021/11/17
 */
public class AviatorRegexFilter {

    private static final String SPLIT = ",";
    private static final String PATTERN_SPLIT = "|";
    private static final String FILTER_EXPRESSION = "regex(pattern,target)";
    private static final RegexFunction regexFunction = new RegexFunction();
    private final Expression exp = AviatorEvaluator.compile(FILTER_EXPRESSION, true);

    static {
        AviatorEvaluator.addFunction(regexFunction);
    }

    private static final Comparator<String> COMPARATOR = new StringComparator();

    final private String pattern;
    final private boolean defaultEmptyValue;

    public AviatorRegexFilter(String pattern) {
        this(pattern, true);
    }

    public AviatorRegexFilter(String pattern, boolean defaultEmptyValue) {
        this.defaultEmptyValue = defaultEmptyValue;
        List<String> list = null;
        if (StringUtils.isEmpty(pattern)) {
            list = new ArrayList<>();
        } else {
            String[] ss = StringUtils.split(pattern, SPLIT);
            list = Arrays.asList(ss);
        }

        list.sort(COMPARATOR);
        list = completionPattern(list);
        this.pattern = StringUtils.join(list, PATTERN_SPLIT);
    }

    public boolean filter(String filtered) throws FilterException {
        if (StringUtils.isEmpty(pattern)) {
            return defaultEmptyValue;
        }

        if (StringUtils.isEmpty(filtered)) {
            return defaultEmptyValue;
        }

        Map<String, Object> env = new HashMap<>();
        env.put("pattern", pattern);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String str1, String str2) {
            return Integer.compare(str2.length(), str1.length());
        }
    }

    private List<String> completionPattern(List<String> patterns) {
        List<String> result = new ArrayList<>();
        for (String pattern : patterns) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("^");
            stringBuffer.append(pattern);
            stringBuffer.append("$");
            result.add(stringBuffer.toString());
        }
        return result;
    }

    @Override
    public String toString() {
        return pattern;
    }
}
