package catalog.conn.imp;

import catalog.conn.Connector;
import catalog.pojo.PojoCatalogTableConnInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DatagenConnectorConstructImp extends Connector {

    public static final String UNI_LABEL = "datagen";

    /**
     * 会引发非兼容属性的元数据, 现在没有用到
     */
    Set<String> flinkOnlyMeta = new HashSet<String>() {{
    }};


    /**
     *
     */
    Set<String> dataGenOption = new HashSet<String>() {{
    }};

    private final static String speHead = "";

    public DatagenConnectorConstructImp() {
        super.flinkOptions.addAll(flinkOnlyMeta);
        super.flinkOptions.addAll(dataGenOption);
    }

    @Override
    public List<PojoCatalogTableConnInfo> constract(List<PojoCatalogTableConnInfo> sou) {
        return sou;
    }
}
