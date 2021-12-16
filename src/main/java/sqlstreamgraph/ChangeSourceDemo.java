package sqlstreamgraph;

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;

/**
 * 通过获取 StreamGraph 实现 sql source 并行度的修改
 * 官方只实现了 sink 并行度的修改： https://issues.apache.org/jira/browse/FLINK-19727
 * sql 代码中 source 并行度的接口是有的，但是 1.14 代码中，只有接口没有具体实现
 */
public class ChangeSourceDemo {

    /**
     * 模拟修改输入并行度
     *
     * @param streamGraph
     * @param parall
     */
    public static void changeSourceParallelism(StreamGraph streamGraph, int parall) {
        // 统一修改 source 并行度
        for (Integer sourceId : streamGraph.getSourceIDs()) {
            StreamNode temp = streamGraph.getStreamNode(sourceId);
            temp.setParallelism(parall);
            // 修改 数据交换规则
            for (StreamEdge edge : temp.getOutEdges()) {
                edge.setPartitioner(new ShufflePartitioner<>());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tableEnvironment.executeSql("CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")");
        tableEnvironment.executeSql("create table out_s (order_number BIGINT) " +
                "with (" +
                "  'connector' = 'blackhole'\n" +
                ")");
        tableEnvironment.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 3);
        RawStatementSetImpl rs = new RawStatementSetImpl((TableEnvironmentInternal) tableEnvironment);
        rs.addInsertSql("insert into out_s select order_number from Orders");
        StreamGraph sg = rs.getGraph();
        changeSourceParallelism(sg, 2);
        rs.execute(sg);
    }
}
