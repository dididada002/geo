package com.geoserver.publish.task;

import it.geosolutions.geoserver.rest.GeoServerRESTManager;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.GeoServerRESTReader;
import it.geosolutions.geoserver.rest.decoder.RESTDataStore;
import it.geosolutions.geoserver.rest.decoder.RESTLayer;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder;
import org.apache.commons.httpclient.NameValuePair;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author: jingteng
 * @date: 2023/3/28 21:23
 */
@Component
public class MyTask {
    /**
     * E:\tet\fuyang-linping
     */
//    @Value("${srcPath}")
//    private String srcPath = "E:\\tet\\fuyang-linping";
    private String srcPath = "E:\\tet\\fuyang-linping\\fuyang\\240_0\\31";

    /**
     * E:\tet\fuyang-linping\pro
     */
//    @Value("${targetPath}")
    private String targetPath = "E:\\tet\\fuyang-linping\\pro\\";

    private String targetZipPath = "E:\\tet\\fuyang-linping\\pro-zip\\";

    String url = "http://182.92.107.134:8050/geoserver";    //geoserver的地址
    String un = "admin";         //geoserver的账号
    String pw = "geoserver";     //geoserver的密码

    String workspace = "jingteng";     //工作区名称
    String storename = "jingteng";     //数据源名称
    String zoneStyle = "zone-test";
    String linkStyle = "link-test";

    public void execute() {
        //首先规整文件
        // src:E:\tet\fuyang-linping
        tranferFile(srcPath);
        //打包
        zipOut(targetPath);
        //发布
        List<String> strings = publishShp(targetZipPath);
        System.out.println(strings);

    }

    public void tranferFile(String path){
//        File file = new File("D:\\private\\linping\\2");
        File file = new File(path);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file1 = files[i];
            transFile(file1);
        }
    }
    void transFile(File file){
        if (file.isDirectory()){
            File[] files = file.listFiles();
            for (File file1 : files) {
                transFile(file1);
            }
            return;
        }
        //path : E:\tet\fuyang-linping\fuyang\240_0\30\0 00_00
        String name = file.getName();
        // E:\tet\fuyang-linping\fuyang\240_0\30\0 00_00\2D Zones.dbf
        String absolutePath = file.getAbsolutePath();
        String[] pathArr = absolutePath.split("\\\\");
        int length = pathArr.length;
        String j = pathArr[length - 4] + "_" + pathArr[length - 3];
        String[] s = pathArr[length - 2].split(" ");
        String strI = absolutePath.contains("Max") ? "max" : String.valueOf(map(s[1]));
        String[] split = name.split("\\.");
        String houzhui = split[1];
//        String baseUrl = "D:\\private\\pro\\";
        if (name.contains("2D Zones")){
            System.out.println("===========zone start============");
            String url = targetPath + j + "_zone_" + strI + "." + houzhui;
            File targetFile = new File(url);
            System.out.println("===========zone start============");
            boolean b = file.renameTo(targetFile);
            System.out.println(b);
        }else if (name.contains("Links")){
            System.out.println("===========link start============");
            String url = targetPath + j + "_link_" + strI + "." + houzhui;
            File targetFile = new File(url);
            boolean b = file.renameTo(targetFile);
            System.out.println("===========link start============");
            System.out.println(b);
        }
        return;
    }

    public int map(String str) {
        String[] parts = str.split("_"); // 将字符串分割成两个部分
        int firstPart = Integer.parseInt(parts[0].trim()); // 解析第一部分数字
        int secondPart = Integer.parseInt(parts[1].trim()); // 解析第二部分数字

        // 计算对应数字
        int result = firstPart * 12 + (secondPart / 5);

        return result;
    }

    /**
     * E:\tet\fuyang-linping\pro
     * @param folderPath
     * @return
     */
    public List<List<File>> classifyFiles(String folderPath) {
        File folder = new File(folderPath);
        File[] files = folder.listFiles(); // 获取文件夹中的所有文件

        Map<String, List<File>> fileMap = new HashMap<>(); // 用于存储相同文件名的文件

        // 遍历所有文件，将相同文件名的文件归类到同一个List中
        for (File file : files) {
            String fileName = file.getName();
            String nameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
            List<File> fileList = fileMap.getOrDefault(nameWithoutExtension, new ArrayList<>());
            fileList.add(file);
            fileMap.put(nameWithoutExtension, fileList);
        }

        // 将所有List添加到一个大的List中
        List<List<File>> result = new ArrayList<>();
        for (String fileName : fileMap.keySet()) {
            List<File> fileList = fileMap.get(fileName);
            result.add(fileList);
        }

        return result;
    }

    public void zipOut(String fileToZipPath){
        List<List<File>> lists = classifyFiles(fileToZipPath);
        lists.forEach(srcFiles -> {
            try {
                String nameWithHouZhui = srcFiles.get(0).getName();
                String name = nameWithHouZhui.split("\\.")[0];
                toZip(targetZipPath + name + ".zip",srcFiles);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        System.out.println(1);
    }

    private final byte[] buf = new byte[1024];
    public  void toZip(String zipFileName, List<File> srcFiles) throws Exception {
        long start = System.currentTimeMillis();
        ZipOutputStream zos = null;
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(zipFileName);
            zos = new ZipOutputStream(fileOutputStream);
            for (File srcFile : srcFiles) {
                compress(srcFile, zos, srcFile.getName(), true);
            }
            long end = System.currentTimeMillis();
            System.out.println("压缩 (" + zipFileName +" ) 完成，耗时：" + (end - start) + " 毫秒");
        } catch (Exception e) {
            throw new RuntimeException("zip error from ZipUtils", e);
        } finally {
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public  void compress(File sourceFile, ZipOutputStream zos, String name,
                          boolean KeepDirStructure) throws Exception {

        if (sourceFile.isFile()) {
            // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
            zos.putNextEntry(new ZipEntry(name));
            // copy文件到zip输出流中
            int len;
            FileInputStream in = new FileInputStream(sourceFile);
            while ((len = in.read(buf)) != -1) {
                zos.write(buf, 0, len);
            }
            // Complete the entry
            zos.closeEntry();
            in.close();
        } else {
            File[] listFiles = sourceFile.listFiles();
            if (listFiles == null || listFiles.length == 0) {
                // 需要保留原来的文件结构时,需要对空文件夹进行处理
                if (KeepDirStructure) {
                    // 空文件夹的处理
                    zos.putNextEntry(new ZipEntry(name + "/"));
                    // 没有文件，不需要文件的copy
                    zos.closeEntry();
                }
            } else {
                for (File file : listFiles) {
                    // 判断是否需要保留原来的文件结构
                    if (KeepDirStructure) {
                        // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
                        // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
                        compress(file, zos, name + "/" + file.getName(), KeepDirStructure);
                    } else {
                        compress(file, zos, file.getName(), KeepDirStructure);
                    }

                }
            }
        }
    }



    /**
     * 将shapefile文件发布为geoserver服务
     *
     * @return
     */
    public List<String> publishShp(String layerZipPath){
        //shp文件压缩包，必须是zip压缩包，且shp文件(.shp、.dbf、.shx等)外层不能有文件夹，且压缩包名称需要与shp图层名称一致
//        String zipFilePath = "D:\\private\\linping-zip\\" + layername + ".zip";
        List<String> failList = new ArrayList<>();
        try {
            //  1、获取geoserver连接对象
            GeoServerRESTManager manager = null;

            try {
                manager = new GeoServerRESTManager(new URL(url) , un , pw);
                System.out.println("连接geoserver服务器成功");
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("geoserver服务器连接失败");
                return failList;
            }

            GeoServerRESTReader reader = manager.getReader();
            GeoServerRESTPublisher publisher = manager.getPublisher();

            //  2、判断是否有工作区，没有则创建
            boolean b2 = reader.existsWorkspace(workspace);
            if(!b2){
                boolean b = publisher.createWorkspace(workspace);
                if(!b){
                    System.out.println("工作区创建失败");
                    return failList;
                }
            }

            //  3、判断是否有数据源，没有则创建
            //  4、发布图层，如果存在就不发布
            //  创建数据源 和 发布图层服务可以一步进行
            File folder = new File(layerZipPath);
            File[] files = folder.listFiles();
            for (File file : files) {
                boolean publishLayerResult = publishLayer(reader, publisher, file);
                if (!publishLayerResult) {
                    failList.add(file.getName());
                }
            }
            return failList;
        }catch (Exception e){
            e.printStackTrace();
            return failList;
        }
    }
    public boolean publishLayer(GeoServerRESTReader reader, GeoServerRESTPublisher publisher, File file) throws FileNotFoundException {
        String name = file.getName();
        String layername = name.split("\\.")[0];
        RESTDataStore datastore = reader.getDatastore(workspace, storename);
        RESTLayer layer = reader.getLayer(workspace, layername);
        if(layer==null || datastore==null){
            // 进行发布；参数依次为：工作区名称、数据源名称、图层名称、shp文件压缩文件对象、坐标系
//                boolean b = publisher.publishShp(workspace , storename , layername , file , "EPSG:3857" , "line-test");
            String style = layername.contains("zone")? zoneStyle: linkStyle;

            boolean b = publisher.publishShp( workspace,  storename, new NameValuePair[]{new NameValuePair("charset", "GBK")},
                    //图层名称               指定用于发布资源的方法
                    layername, it.geosolutions.geoserver.rest.GeoServerRESTPublisher.UploadMethod.FILE,
                    //        zip图集的地址           坐标系         样式
                    file.toURI(),"EPSG:3857", "EPSG:3857", GSResourceEncoder.ProjectionPolicy.FORCE_DECLARED,style);
            if(!b){
                System.out.println("shp图层发布失败");
                return false;
            }
            System.out.println("*********************shp图层发布成功");
        }

        return true;
    }
}
