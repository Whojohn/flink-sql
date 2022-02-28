package catalog.conn;

import catalog.pojo.PojoCatalogTableConnInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class Connector implements FlinkConnectorConstruct {
    public Set<String> flinkOptions = new HashSet<String>() {{
        add("connector");
    }};

    public List<PojoCatalogTableConnInfo> addHead(List<PojoCatalogTableConnInfo> sou, Set<String> flinkOpt, String specHead) {
        for (int loc = 0; loc < sou.size(); loc++) {
            PojoCatalogTableConnInfo tem = sou.get(loc);
            if (!flinkOpt.contains(tem.getKey())) {
                sou.set(loc, new PojoCatalogTableConnInfo(specHead + tem.getKey(), tem.getValue()));
            }
        }
        return sou;
    }

}
