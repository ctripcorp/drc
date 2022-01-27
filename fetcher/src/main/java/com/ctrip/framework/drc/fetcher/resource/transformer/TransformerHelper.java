package com.ctrip.framework.drc.fetcher.resource.transformer;

import com.ctrip.framework.drc.core.filter.MigrateMap;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.oro.text.regex.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jixinwang on 2022/1/12
 */
public class TransformerHelper {

    // match offer[1-128]
    public static final String MODE_PATTERN = "(.*)(\\[(\\d+)\\-(\\d+)\\])(.*)";
    private static Map<String, Pattern> patterns = MigrateMap.makeComputingMap(new Function<String, Pattern>() {
        public Pattern apply(String input) {
            PatternCompiler pc = new Perl5Compiler();
            try {
                return pc.compile(input,
                        Perl5Compiler.CASE_INSENSITIVE_MASK
                                | Perl5Compiler.READ_ONLY_MASK);
            } catch (MalformedPatternException e) {
                throw new RuntimeException(e);
            }
        }
    });

    public static List<String> parseMode(String value) {
        PatternMatcher matcher = new Perl5Matcher();
        if (matcher.matches(value, patterns.get(MODE_PATTERN))) {
            MatchResult matchResult = matcher.getMatch();
            String prefix = matchResult.group(1);
            String startStr = matchResult.group(3);
            String ednStr = matchResult.group(4);
            int start = Integer.valueOf(startStr);
            int end = Integer.valueOf(ednStr);
            String postfix = matchResult.group(5);

            List<String> values = new ArrayList<String>();
            for (int i = start; i <= end; i++) {
                StringBuilder builder = new StringBuilder(value.length());
                String str = String.valueOf(i);
                // handle 0001 type
                if (startStr.length() == ednStr.length() && startStr.startsWith("0")) {
                    str = StringUtils.leftPad(String.valueOf(i), startStr.length(), '0');
                }

                builder.append(prefix).append(str).append(postfix);
                values.add(builder.toString());
            }
            return values;
        } else {
            return Lists.newArrayList();
        }
    }
}
