package pa2.lemma;

import java.util.LinkedList;
import java.util.List;

public class SpliteUtil {
    private static String spliter="\\|+|\\s+|\\-+|\\——+|\\[+|\\]+|\\(+|\\)+|\\{+|\\}+|#+|=+|:+|\\.|:+|,+|\\*+|\\;+";
    public static List<String> getTokerns(String str){
        List<String> list=new LinkedList<>();
        String[] arr=str.split(spliter);
        for(String s:arr){
            list.add(s);
        }
        return list;
    }
}
