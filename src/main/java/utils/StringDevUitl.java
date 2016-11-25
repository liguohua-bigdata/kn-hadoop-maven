package utils;
public class StringDevUitl {
    public static boolean isNotEmputy(String str) {
        return !isEmputy(str);
    }
    public static boolean isEmputy(String str) {
        if ("".equals(str.trim()) || str == null) {
            return true;
        } else {
            return false;
        }
    }
}
