package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class HdfsDevUtil {

    public static final Configuration conf = new Configuration();

    public static FileSystem getFilesystem(String path) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(path), conf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    public static void deleteFileOnExist(String path) {
        try {
            FileSystem filesystem = getFilesystem(path);
            if (filesystem.exists(new Path(path))) {
                filesystem.delete(new Path(path), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void renameFileOnExist(String path) {
        try {
            FileSystem filesystem = getFilesystem(path);
            Path path0 = new Path(path);
            if (filesystem.exists(path0)) {
                filesystem.rename(path0, new Path(path + "-" + new Date().getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Set<String> readFile2Set(String path) {
        Set<String> hashSet = new HashSet<>();
        List<String> list = readFile2List(path);
        for (String s : list) {
            hashSet.add(s);
        }
        return hashSet;
    }

    public static List<String> readFile2List(String path) {
        List<String> list = new ArrayList<>();
        FileSystem fs = null;
        FSDataInputStream hdfsInStream = null;
        try {
            fs = FileSystem.get(new URI(path), conf);
            hdfsInStream = fs.open(new Path(path));

            BufferedReader in = null;
            in = new BufferedReader(new InputStreamReader(hdfsInStream, "UTF-8"));
            for (String line = null; (line = in.readLine()) != null; ) {
                list.add(line);
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hdfsInStream.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return list;
    }
}
