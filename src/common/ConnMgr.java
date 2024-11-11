package common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnMgr {

    private static final Logger logger = LogManager.getLogger(ConnMgr.class);

    private Connection conn = null;

    public Connection getConnection() {
        try {
            if (conn == null || conn.isClosed()) {
                conn = this.getConnectMethod();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return conn;
    }

    public Connection getConnectMethod() {
        Connection conn = null;
        try {
            String DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            String url = "";
            String user = "";
            String password = "";

            Class.forName(DRIVER);
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        return conn;

    }
}
