package catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Handler;

public class TestFlow {
    public static void main(String[] args) throws ClassNotFoundException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.registerCatalog("test", new MyCatalog("test", "test", "root", "123456", "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&useSSL=false&characterEncoding=utf-8&serverTimezone=UTC"));
        tableEnvironment.executeSql("USE CATALOG test");

        tableEnvironment.executeSql("CREATE TABLE datagen_sou (_useless_ TIMESTAMP(3)  ) WITH ()");
        tableEnvironment.executeSql("DESCRIBE  datagen_sou").print();
        tableEnvironment.executeSql("create view tem as select map_map_nested from datagen_sou");
        tableEnvironment.executeSql("show tables").print();
        tableEnvironment.executeSql("DESCRIBE ka_test").print();
        tableEnvironment.executeSql("select * from ka_test").print();
    }
}
