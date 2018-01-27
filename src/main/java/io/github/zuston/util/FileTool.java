package io.github.zuston.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuston on 2018/1/27.
 */
public class FileTool {
    public static boolean createDir(String path){
        File myFolderPath = new File(path);
        try {
            if (!myFolderPath.exists()){
                return myFolderPath.mkdirs();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static String getSimpleDirName(String parentPath){
        File parentFile = new File(parentPath);
        File[] tempList = parentFile.listFiles();
        if (tempList.length == 1 && tempList[0].isDirectory()){
            return  tempList[0].getName();
        }
        return null;
    }

    public static List<String> getDirNames(String parentPath){
        List<String> dirList = new ArrayList<String>();
        File parentFile = new File(parentPath);
        File[] tempList = parentFile.listFiles();
        for (File temp : tempList){
            if (temp.isDirectory()) dirList.add(temp.getName());
        }
        return dirList;
    }

    public static void delete(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (int i = 0; i < files.length; i++) {
            delete(files[i]);
        }
        path.delete();
    }

    public static void main(String[] args) {
//        boolean a = createDir("tmp/2017-10-10");
//        delete(new File("tmp"));
        System.out.println();
        getDirNames("tmp");
    }
}
