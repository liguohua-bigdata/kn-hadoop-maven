package test.java;

import java.io.File;
import java.util.Scanner;
import java.util.jar.JarFile;

/**
 * Created by liguohua on 16/10/3.
 * 此类用于测试，Java读取jar包内的文件
 * 将文件打入jar包
 * 1.将文件放入src/main/resources目录中
 * 2.使用maven进行打包
 */
public class Test {
    public  static void main(String[] args) throws  Exception{
        String PEOPLE_FILE="people.txt";
        ClassLoader classLoader = Test.class.getClassLoader();
        String fileUrl=classLoader.getResource(PEOPLE_FILE).getFile();
        System.out.println(fileUrl);

        String jarFile=fileUrl.substring(5,(fileUrl.length()-PEOPLE_FILE.length()-2));
        System.out.println(jarFile);
        JarFile jf = new JarFile(new File(jarFile));
        Scanner scanner=new Scanner(jf.getInputStream(jf.getEntry(PEOPLE_FILE)));
        while (scanner.hasNext()){
            String line=scanner.nextLine();
            System.out.println(line);

        }

    }

}
