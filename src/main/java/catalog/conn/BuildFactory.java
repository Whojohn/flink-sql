package catalog.conn;

import catalog.conn.imp.DatagenConnectorConstructImp;
import catalog.conn.imp.KafkaConnectorConstructImp;
import catalog.pojo.PojoCatalogTableConnInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BuildFactory {
    public static final Map<String, FlinkConnectorConstruct> flinkConClass = new HashMap<String, FlinkConnectorConstruct>() {{
        put(KafkaConnectorConstructImp.UNI_LAB, new KafkaConnectorConstructImp());
        put(DatagenConnectorConstructImp.UNI_LABEL, new DatagenConnectorConstructImp());
    }};

    public List<PojoCatalogTableConnInfo> buildFlinkConn(List<PojoCatalogTableConnInfo> sou) throws Exception {
        List<PojoCatalogTableConnInfo> conType = sou.stream().
                filter(e -> e.getKey().equals(PojoCatalogTableConnInfo.META_SPEC_FLINK_CONNECTOR)).
                collect(Collectors.toList());
        if (conType.size() != 1) {
            throw new UnsupportedOperationException("Connector type accept only one, but has " + conType.toString());
        } else if (!flinkConClass.containsKey(conType.get(0).getValue())) {
            throw new UnsupportedOperationException("This connector is not contains in factory" + conType.toString());
        }

        return flinkConClass.get(
                conType.get(0).getValue())
                .constract(sou);
    }
}
