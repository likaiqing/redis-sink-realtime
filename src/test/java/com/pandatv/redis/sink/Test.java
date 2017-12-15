package com.pandatv.redis.sink;

import com.google.common.collect.Lists;
import org.jboss.netty.handler.codec.base64.Base64Decoder;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Created by likaiqing on 2017/10/9.
 */
public class Test {
    @org.junit.Test
    public void test1(){
        IntStream.range(-3, 0).forEach(i->System.out.print(i));
    }
    @org.junit.Test
    public void test2(){
        DateTimeFormatter stf = DateTimeFormat.forPattern("yyyyMMddHHmm");
        long l = Long.parseLong(stf.print(stf.parseDateTime(String.valueOf("201710091611")).plusMinutes(-3)));
        System.out.println(l);
    }
    @org.junit.Test
    public void test3(){
        String v = "/data/logs/current/charge/2017.*/.*expendnew.log";
        File f = new File(v);
        File parentDir = f.getParentFile();//columnKey
        Pattern filenamePattern = Pattern.compile(f.getName());//value

        File paParentDir = parentDir.getParentFile();
        Pattern parentDirPattern = Pattern.compile(parentDir.getName());

        List filelist=getMatchDirectory(paParentDir, parentDirPattern);
    }

    private List getMatchDirectory(File parentDir, final Pattern fileNamePattern) {
        FileFilter filter = new FileFilter() {
            public boolean accept(File f) {
                String fileName = f.getName();
                if (!f.isDirectory() || !fileNamePattern.matcher(fileName).matches()) {
                    return false;
                }
                return true;
            }
        };
        File[] files = parentDir.listFiles(filter);
        if(files.length==0)return null;
        ArrayList<File> result = Lists.newArrayList(files);
//        Collections.sort(result, new TailFile.CompareByLastModifiedTime());
        return result;
    }
    @org.junit.Test
    public void test4(){//5pe256m65peF6KGM56S85YyF:旅行时空包
        String decode = Base64.getDecoder().decode("5pe256m65peF6KGM56S85YyF").toString();
        System.out.println(decode);
    }
}
