import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class TestSqlAutoComplete {
    Logger log = Logger.getLogger(TestSqlAutoComplete.class.toString());

    private static final String benchTest = "SELECT b, count(a), sum(c), sum(d)\n" +
            "FROM (\n" +
            "SELECT T.* FROM (\n" +
            "SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proctime\n" +
            "FROM T1\n" +
            "GROUP BY a, b\n" +
            "      ) AS T\n" +
            "JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D\n" +
            "ON T.a = D.id\n" +
            "WHERE D.age > 10\n" +
            "      ) AS T\n" +
            "GROUP BY b";
    ;

    // For testing notice preview sql .
    private static final String testSourceSql = "CREATE TABLE source_table (\n" +
            "    user_id INT,\n" +
            "    cost DOUBLE,\n" +
            "    ts AS localtimestamp,\n" +
            "    WATERMARK FOR ts AS ts\n" +
            ") WITH (\n" +
            "    'connector' = 'datagen',\n" +
            "    'rows-per-second'='5',\n" +
            "\n" +
            "    'fields.user_id.kind'='random',\n" +
            "    'fields.user_id.min'='1',\n" +
            "    'fields.user_id.max'='10',\n" +
            "\n" +
            "    'fields.cost.kind'='random',\n" +
            "    'fields.cost.min'='1',\n" +
            "    'fields.cost.max'='100'\n" +
            ")";

    private final List<String> functionTest = new ArrayList<String>() {{
        add("S");
        add("arom");
        add("form");
        add("oelect");
        add("sel");
        add("seloct");
        add("select * f");
        add("select * frem");
        add("select * from source_tab");
        add("select * from source_table whore");
        add("select * from source_table where user_i");
        add("select * from source_table where user_id = 12 group by ");
        add("select * from source_table where user_id = 12 group by substring(");
        add("select * from source_table where user_id = 12 group by substring (");

    }};

    /**
     * Flink sql client implement.
     * 功能性测试
     */
    @Test
    public void testFlinkImplementFunction() {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tableEnvironment.executeSql(testSourceSql);
        functionTest.forEach(e -> log.info(Arrays.toString(tableEnvironment.getCompletionHints(e, e.length()))));

        // return
        // []                                                           # we want select
        // [FETCH, FROM]                                                # perfect
        // []                                                           # maybe from is better choice
        // [default_catalog.default_database.source_table]              # perfect
        // []                                                           # we want where
        // [user_id]                                                    # perfect
        // [xxxxx]                                                      # notice not support typing whitespace
        // [xxxxx]                                                      # notice not support while use udf
        // [xxxxx]                                                      # notice not support character notice
    }

    /**
     * FST tree implement
     * FST 树初步实现，初步实现第一阶段.不考虑树中间节点可能为一个词，不考虑词频等场景，考虑词频后会更加精确
     * 该部分现在无关返回较多，后续会引入词频统计（常用词靠前解决）
     */
    @Test
    public void testSqlAutoCompleteImp() {
        SqlAutoComplete sqlAutoComplete = new SqlAutoComplete();
        functionTest.forEach(e -> log.info(sqlAutoComplete.getComplete(e).toString()));

        // return
        // [select, self]                                               # perfect
        // [frac_second, ... from   ]                                   # perfect
        // [free]                                                       # maybe from is better choice
        // []                                                           # need sql parse support, we want table name
        // [whenever, where]                                            # perfect
        // [user_defined_type_name, user_defined_type_catalog]          # need sql parse support,we want notice user_id
        // []                                                           # perfect
        // substring                                                    # we want substring()
        // [()]                                                         # perfect
    }

    /**
     * cast time benchmark. testSqlAutoCompleteImp is better.
     */
    @Test
    public void benchMark() {
        long start = System.currentTimeMillis();
        log.info("Test testFlinkImplementFunction");
        IntStream.range(0, 100).forEach(e -> this.testFlinkImplementFunction());
        log.info("testFlinkImplementFunction method cast:" + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        log.info("Test testSqlAutoCompleteImp");
        IntStream.range(0, 100).forEach(e -> this.testSqlAutoCompleteImp());
        log.info("testSqlAutoCompleteImp method cast:" + (System.currentTimeMillis() - start));


    }


}
