package catalog.pojo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PojoCatalogTableConnInfo {
    Integer tableId;
    String key;
    String value;
    String comment;


    public PojoCatalogTableConnInfo(String key, String value ) {
        this.key = key;
        this.value = value;
    }

    public static List<PojoCatalogTableConnInfo> getByTableId(Connection conn, int tableId) throws SQLException {
        List<PojoCatalogTableConnInfo> retList = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement("SELECT `key`,`value` FROM catalog_table_conn_info where table_id  = ?;");
        ps.setInt(1, tableId);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            retList.add(new PojoCatalogTableConnInfo(rs.getString("key"),
                    rs.getString("value")));
        }
        return retList;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
