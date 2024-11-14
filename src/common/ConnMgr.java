package common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnMgr {
    private static final Logger logger = LogManager.getLogger(ConnMgr.class);
    private static HikariDataSource dataSource;

    static {
        try {
            HikariConfig config = new HikariConfig();
            config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            config.setJdbcUrl("");
            config.setUsername("");
            config.setPassword("");

            config.setMaximumPoolSize(10);
            config.setReadOnly(true);
            config.setAutoCommit(false);

            dataSource = new HikariDataSource(config);
        } catch (Exception e) {
            logger.error("Error initializing HikariCP connection pool", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        if (dataSource != null) {
            Connection conn = dataSource.getConnection();
            if (conn == null) {
                logger.error("Failed to get database connection.");
                throw new RuntimeException("Failed to get database connection.");
            }
            return conn;
        } else {
            logger.error("DataSource is not initialized.");
            throw new RuntimeException("DataSource is not initialized.");
        }
    }

    public static void closePool() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
