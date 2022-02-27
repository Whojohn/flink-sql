package catalog.pojo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PojoCatalogTableSchema {
    Integer tableId;
    Short schemaRoot;
    String schemaName;
    String schemaType;
    String comment;


    public static final String META_TABLE_NAME = "catalog_database";

    public PojoCatalogTableSchema(Short schemaRoot, String schemaName, String schemaType) {
        this.schemaRoot = schemaRoot;
        this.schemaName = schemaName;
        this.schemaType = schemaType;
    }

    public static List<PojoCatalogTableSchema> getByTableId(Connection conn, int tableId) throws SQLException {
        List<PojoCatalogTableSchema> retList = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement("select schema_root,schema_name,schema_type from test.catalog_table_schema where table_id = ? ;");
        ps.setInt(1, tableId);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            retList.add(new PojoCatalogTableSchema(rs.getShort("schema_root"),
                    rs.getString("schema_name"),
                    rs.getString("schema_type")));
        }
        return retList;
    }

    public Integer getByTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public Short getSchemaRoot() {
        return schemaRoot;
    }

    public void setSchemaRoot(Short schemaRoot) {
        this.schemaRoot = schemaRoot;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public static String getMetaTableName() {
        return META_TABLE_NAME;
    }
}
