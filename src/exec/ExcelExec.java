package exec;

import common.ConnMgr;
import common.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExcelExec {
    private static final Logger logger = LogManager.getLogger(ExcelExec.class);
    //多張表名
    private static final String[] list = new String[] {
            "",
            "",
            ""
    };
    //單次查詢筆數
    private static final int batchSize = 10000;
    //Excel限制行數1,048,576, 這邊設成上限1,000,000, 超過就會分割文件
    private static final int rowLimit = 1000000;

    public static void main(String[] args) {
        ExcelExec excel = new ExcelExec();
        excel.processTables();
    }

    public void processTables() {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            //每張表分thread跑
            for (String table : list) {
                executorService.submit(() -> processTableInBatches(table));
            }

            executorService.shutdown();
            boolean result = executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info("Job-OK: " + result);
            ConnMgr.closePool();
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    private void processTableInBatches(String tableName) {
        try (Connection conn = ConnMgr.getConnection();
             Statement countStmt = Objects.requireNonNull(conn).createStatement();
             ResultSet rs = countStmt.executeQuery("SELECT COUNT(1) FROM " + tableName)) {

            rs.next();
            int totalRows = rs.getInt(1);
            //總行數÷行數上限
            int threadsRequired = (int) Math.ceil((double) totalRows / rowLimit);
            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            for (int i = 0; i < threadsRequired; i++) {
                int fileCount = i + 1;
                int startOffset = i * rowLimit;
                int endOffset = Math.min(startOffset + rowLimit, totalRows);
                executor.submit(() -> exec(tableName, startOffset, endOffset, fileCount));
            }

            executor.shutdown();
            boolean result = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info(tableName + "-OK: " + result);
        } catch (SQLException | InterruptedException e) {
            logger.error("Error processing table " + tableName, e);
        }
    }

    private void exec(String tableName, int startOffset, int endOffset, int fileIndex) {
        int offset = startOffset;
        try (Connection conn = ConnMgr.getConnection();
             SXSSFWorkbook workbook = new SXSSFWorkbook(100)) {
            //避免Excel分頁名過長，限制31字
            String sheetName = tableName.length() > 31 ? tableName.substring(0, 31) : tableName;
            SXSSFSheet sheet = workbook.createSheet(sheetName);
            int rowNum = 0;

            while (offset < endOffset) {
                String sql = "SELECT * FROM " + tableName + " ORDER BY (SELECT NULL) OFFSET " + offset + " ROWS FETCH NEXT " + batchSize + " ROWS ONLY";
                try (Statement stmt = Objects.requireNonNull(conn).createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    if (!rs.next()) {
                        break;
                    }

                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    if (rowNum == 0) {
                        //第一行寫欄位名
                        addColumnName(sheet, rowNum, columnCount, metaData);
                        rowNum++;
                    }

                    do {
                        Row row = sheet.createRow(rowNum++);
                        for (int i = 1; i <= columnCount; i++) {
                            Cell cell = row.createCell(i - 1);
                            String result = rs.getString(i);
                            if(result != null) {
                                cell.setCellValue(StringUtil.decodeBig5Word(result));
                            }
                        }

                        if (rowNum % batchSize == 0) {
                            sheet.flushRows();
                        }
                    } while (rs.next());

                    offset += batchSize;
                }
            }
            saveWorkbook(workbook, tableName, fileIndex);
        } catch (SQLException | IOException e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    private void addColumnName(SXSSFSheet sheet, int rowNum, int columnCount, ResultSetMetaData metaData) throws SQLException {
        Row headerRow = sheet.createRow(rowNum);
        for (int i = 1; i <= columnCount; i++) {
            Cell cell = headerRow.createCell(i - 1);
            cell.setCellValue(metaData.getColumnName(i));
        }
    }

    public void saveWorkbook(SXSSFWorkbook workbook, String tableName, int fileIndex) throws IOException {
        try (FileOutputStream fileOut = new FileOutputStream(tableName + "_" + fileIndex + ".xlsx")) {
            workbook.write(fileOut);
        }
        logger.info(tableName + "-" + fileIndex + ": OK");
    }
}