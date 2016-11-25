package utils;


import java.io.File;
import java.util.Date;

public class FileDevUtil {
    public static void deleteFile(String filePath) {
        File file = new File(filePath);
        file.deleteOnExit();
    }

    public static void removeFileName(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            file.renameTo(new File(file.getAbsolutePath() + "-" + new Date().getTime()));

        }
    }
}
