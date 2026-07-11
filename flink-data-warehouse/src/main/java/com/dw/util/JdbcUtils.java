package com.dw.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL JDBC 工具类 — 连接管理 + 批量 UPSERT
 */
public final class JdbcUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

    private JdbcUtils() {}

    /**
     * 获取数据库连接
     */
    public static Connection getConnection(String jdbcUrl, String user, String password) {
        try {
            return DriverManager.getConnection(jdbcUrl, user, password);
        } catch (SQLException e) {
            LOG.error("Failed to connect to PostgreSQL: {}", jdbcUrl, e);
            throw new RuntimeException("PostgreSQL connection failed", e);
        }
    }

    /**
     * 批量执行 UPSERT（INSERT ... ON CONFLICT ... DO UPDATE）
     *
     * @param conn   数据库连接
     * @param sql    UPSERT SQL 语句
     * @param params 参数列表，每个 Object[] 对应一条记录
     */
    public static void executeBatchUpsert(Connection conn, String sql, List<Object[]> params) {
        if (params == null || params.isEmpty()) {
            return;
        }
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (Object[] row : params) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            conn.commit();
            LOG.info("Batch upsert {} rows executed.", params.size());
        } catch (SQLException e) {
            LOG.error("Batch upsert failed", e);
            try { conn.rollback(); } catch (SQLException ignored) {}
        }
    }

    /**
     * 关闭连接
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }
}
