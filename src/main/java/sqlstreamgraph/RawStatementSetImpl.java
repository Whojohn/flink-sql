package sqlstreamgraph;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RawStatementSetImpl implements StatementSet {
    private final TableEnvironmentInternal tableEnvironment;
    private List<ModifyOperation> operations = new ArrayList<>();
    StreamGraph streamGraph = null;
    Executor exeEnv;

    public RawStatementSetImpl(TableEnvironmentInternal tableEnvironment) {
        this.tableEnvironment = tableEnvironment;
    }

    @Override
    public StatementSet addInsertSql(String statement) {
        List<Operation> operations = tableEnvironment.getParser().parse(statement);

        if (operations.size() != 1) {
            throw new TableException("Only single statement is supported.");
        }

        Operation operation = operations.get(0);
        if (operation instanceof ModifyOperation) {
            this.operations.add((ModifyOperation) operation);
        } else {
            throw new TableException("Only insert statement is supported now.");
        }
        return this;
    }

    @Override
    public StatementSet addInsert(String targetPath, Table table) {
        return addInsert(targetPath, table, false);
    }

    @Override
    public StatementSet addInsert(String targetPath, Table table, boolean overwrite) {
        UnresolvedIdentifier unresolvedIdentifier =
                tableEnvironment.getParser().parseIdentifier(targetPath);
        ObjectIdentifier objectIdentifier =
                tableEnvironment.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        operations.add(
                new CatalogSinkModifyOperation(
                        objectIdentifier,
                        table.getQueryOperation(),
                        Collections.emptyMap(),
                        overwrite,
                        Collections.emptyMap()));

        return this;
    }

    @Override
    public String explain(ExplainDetail... extraDetails) {
        List<Operation> operationList =
                operations.stream().map(o -> (Operation) o).collect(Collectors.toList());
        return tableEnvironment.explainInternal(operationList, extraDetails);
    }

    public StreamGraph getGraph() throws Exception {
        Class cls = tableEnvironment.getClass();
        Method methodTranslate = cls.getDeclaredMethod("translate", List.class);
        methodTranslate.setAccessible(true);


        Field ex = cls.getDeclaredField("execEnv");
        ex.setAccessible(true);
        this.exeEnv = (Executor) ex.get(tableEnvironment);
        List<Transformation<?>> transformations = (List<Transformation<?>>) methodTranslate.invoke(tableEnvironment, this.operations);
        this.streamGraph = (StreamGraph) this.exeEnv.createPipeline(transformations, tableEnvironment.getConfig(), "");
        return this.streamGraph;
    }

    public TableResult execute(StreamGraph sg) throws Exception {
        return (TableResult) this.exeEnv.executeAsync(sg);
    }


    @Override
    public TableResult execute() {
        try {
            return tableEnvironment.executeInternal(operations);
        } finally {
            operations.clear();
        }
    }
}
