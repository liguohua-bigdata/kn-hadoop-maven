package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FileUtil {
    public static Set<String> convertLines2Set(String filePath) {
        BufferedReader br = null;
        Set<String> fileSet = new HashSet<>();
        try {

            File file = new File(filePath);
            br = new BufferedReader(new FileReader(file));
            // read file into memory
            for (String line = null; ((line = br.readLine()) != null); ) {
                fileSet.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        //return set
        return fileSet;
    }


}
