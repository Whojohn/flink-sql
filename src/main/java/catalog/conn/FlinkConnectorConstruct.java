package catalog.conn;

import catalog.pojo.PojoCatalogTableConnInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * @author john
 */
public interface FlinkConnectorConstruct {

//    Set<String> flinkOptions = new HashSet<String>() {{
//        add("connector");
//    }};
//
//    default void addFlinkOptions(Set<String> sou) {
//        flinkOptions.addAll(sou);
//    }

    /**
     * 元数据中存放的连接信息是消除了 flink 特有前缀，现在要按照特定的规则把 flink 前缀补充回去
     *
     * @return
     */
    List<PojoCatalogTableConnInfo> constract(List<PojoCatalogTableConnInfo> sou) throws Exception;


}
