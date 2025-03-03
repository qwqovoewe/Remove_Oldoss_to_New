package com.wrx;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OssBatchMigrationWithDB {
    // 旧 OSS 地址前缀eg：https://xzw.oss-cn-henan-lr.aliyuncs.com/
    private static final String OLD_OSS_PREFIX = "https://OLD_OSS_PREFIX/";
    // 新 OSS 地址前缀
    private static final String NEW_OSS_PREFIX = "https://NEW_OSS_PREFIX/";

    // 旧 OSS 配置（仅用于下载）
    private static final String OLD_BUCKET_NAME = "OLD_BUCKET_NAME";

    // 新 OSS 配置（上传）//
    private static final String NEW_ENDPOINT = "https://NEW_ENDPOINT";
    private static final String NEW_BUCKET_NAME = "NEW_BUCKET_NAME";
    private static final String NEW_ACCESS_KEY_ID = "NEW_ACCESS_KEY_ID";
    private static final String NEW_ACCESS_KEY_SECRET = "NEW_ACCESS_KEY_SECRET";

    // 数据库配置
    private static final String DB_URL = "jdbc:mysql://DB_URL/DBTABLE?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8";
    private static final String DB_USER = "DB_USER";
    private static final String DB_PASSWORD = "DB_PASSWORD";

    //将需要更新的oss所在的列名和表名替换上去
    private static final String TABLE_NAME = "TABLE_NAME";//""your_table";
    private static final String COLUMN_NAME = "COLUMN_NAME";//"file_url"

    // 全局 OSS 客户端
    private static final OSS ossClient = new OSSClientBuilder().build(NEW_ENDPOINT, NEW_ACCESS_KEY_ID, NEW_ACCESS_KEY_SECRET);

    public static void main(String[] args) {
        // 获取数据库中的所有 URL
        List<String> oldUrls = getOldUrlsFromDatabase();

        // 创建线程池进行批量迁移
        ExecutorService executor = Executors.newFixedThreadPool(10);

        for (String oldUrl : oldUrls) {
            executor.submit(() -> migrateFile(oldUrl));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 关闭 OSS 客户端
        ossClient.shutdown();

        System.out.println("迁移完成");
    }


    /**
     * 从数据库获取所有旧的文件 URL
     */
    private static List<String> getOldUrlsFromDatabase() {
        List<String> urls = new ArrayList<>();
        String query = "SELECT "+COLUMN_NAME+" FROM "+TABLE_NAME+" WHERE "+COLUMN_NAME+" LIKE '" + OLD_OSS_PREFIX + "%'";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                urls.add(rs.getString(COLUMN_NAME));
            }
        } catch (SQLException e) {
            // 新增异常处理
            if(e.getMessage().contains("Unknown column")) {
                System.err.println("填写的列名或表名有误，请重新填写");
            }

            e.printStackTrace();
        }
        return urls;
    }

    /**
     * 迁移单个文件
     */
    private static void migrateFile(String oldUrl) {
        try {
            // 获取文件名
            String fileName = oldUrl.replace(OLD_OSS_PREFIX, "");

            //用try-with-resources关闭InputStream
            try (InputStream inputStream = new URL(oldUrl).openStream()) {
                // 上传文件到新 OSS
                ossClient.putObject(NEW_BUCKET_NAME, fileName, inputStream);
            } catch (UnknownHostException e) {
                System.err.println("网络异常，跳过当前文件: " + oldUrl);
                e.printStackTrace();
            }  catch (FileNotFoundException e) {
            // 新增对文件不存在的处理
            System.err.println("文件损坏或不存在或oss连接异常: " + oldUrl);
            e.printStackTrace();
        }
            // 生成新的 URL
            String newUrl = NEW_OSS_PREFIX + fileName;
            System.out.println("迁移成功: " + newUrl);

            // 更新数据库
            updateDatabaseUrl(oldUrl, newUrl);
        } catch (Exception e) {
            System.err.println("迁移失败: " + oldUrl);

            e.printStackTrace();
        }
    }

    /**
     * 更新数据库中的文件 URL
     */
    private static void updateDatabaseUrl(String oldUrl, String newUrl) {
        //将需要更新的oss所在的列名和表名替换上去
        String updateQuery = "UPDATE " + TABLE_NAME + " SET " + COLUMN_NAME + " = ? WHERE " + COLUMN_NAME + " = ?";
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(updateQuery)) {

            pstmt.setString(1, newUrl);
            pstmt.setString(2, oldUrl);
            int rowsUpdated = pstmt.executeUpdate();
            
            if (rowsUpdated > 0) {
                System.out.println("数据库更新成功: " + oldUrl + " -> " + newUrl);
            } else {
                System.out.println("未找到 URL 进行更新: " + oldUrl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
