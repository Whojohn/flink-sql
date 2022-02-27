package catalog.pojo;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PojoCatalogDatabase {
    Integer tableId;
    String dbName;
    String tableName;
    public static final String META_TABLE_NAME = "catalog_database";

    public static List<PojoCatalogDatabase> getByTableId(Connection conn, String dbName) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("select table_id,table_name from catalog_database where db_name = ? ;");
        ps.setString(1, dbName);
        ResultSet rs = ps.executeQuery();
        List<PojoCatalogDatabase> retList = new ArrayList<>();
        while (rs.next()) {
            retList.add(new PojoCatalogDatabase(rs.getInt("table_id"), rs.getString("table_name")));
        }
        return retList;
    }


    public Integer getTableId() {
        return tableId;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public PojoCatalogDatabase(Integer tableId, String tableName) {
        this.tableId = tableId;
        this.tableName = tableName;
    }
}
