package exec;

import common.ConnMgr;
import common.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class ExcelExec {
    private static final Logger logger = LogManager.getLogger(ExcelExec.class);

    public static void main(String[] args) {
        ExcelExec excel = new ExcelExec();
        String tableName = "";
        excel.exec(tableName);
    }

    public void exec(String tableName) {
        ConnMgr cModel = new ConnMgr();
        String sql = "SELECT * FROM " + tableName;
        try (
                FileOutputStream fileOut = new FileOutputStream(tableName + ".xlsx");
                Connection conn = cModel.getConnection();
                Statement stmt = conn.createStatement();
                Workbook workbook = new XSSFWorkbook();
                ResultSet rs = stmt.executeQuery(sql)) {


            CellStyle textStyle = workbook.createCellStyle();
            textStyle.setDataFormat(workbook.createDataFormat().getFormat("@"));
            Sheet sheet = workbook.createSheet(tableName);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            Row headerRow = sheet.createRow(0);
            for (int i = 1; i <= columnCount; i++) {
                Cell cell = headerRow.createCell(i - 1);
                cell.setCellValue(metaData.getColumnName(i));
            }

            int rowNum = 1;
            while (rs.next()) {
                Row row = sheet.createRow(rowNum++);
                for (int i = 1; i <= columnCount; i++) {
                    Cell cell = row.createCell(i - 1);
                    cell.setCellStyle(textStyle);
                    String value = rs.getString(i);
                    if (value != null) {
                        cell.setCellValue(StringUtil.decodeBig5Word(value));
                    }
//                    else {
//                        cell.setCellValue("null");
//                    }
                }
            }
            workbook.write(fileOut);
            System.out.println(tableName + " OK");
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
