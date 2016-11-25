package pa2.lemma;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternUtil {
    private static final List<Pattern> patterns = new LinkedList<>();
    static {
        // urls pattern
        patterns.add(Pattern.compile("((https?://www\\.)|(https?://)|(www\\.))\\w+\\.[a-z]+(/\\w+)*"));
        // references pattern
        patterns.add(Pattern.compile("&lt;ref&gt;.+?&lt;/ref&gt;"));
        // italic, bold pattern
        patterns.add(Pattern.compile("''+"));
        // 's and s' suffixes pattern
        patterns.add(Pattern.compile("('s|s')\\s"));
        // html encoding pattern
        patterns.add(Pattern.compile("&[a-z]+?;"));
        // numbers pattern
        patterns.add(Pattern.compile("[0-9]+?"));
    }

    /**
     * use to check the string is must be remove
     *
     * @param str str
     * @return is must be remove
     */
    public static boolean mustRemove(String str) {
        for (Pattern pattern : patterns) {
            Matcher m = pattern.matcher(str);
            while (m.find()) {
                return true;
            }
        }
        return false;
    }
}
