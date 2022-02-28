package catalog.conn.imp;

import catalog.conn.Connector;
import catalog.pojo.PojoCatalogTableConnInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author john
 */
public class KafkaConnectorConstructImp extends Connector {
    public static final String UNI_LAB = "kafka";

    /**
     *  会引发非兼容属性的元数据, 现在没有用到
     */
    Set<String> flinkOnlyMeta = new HashSet<String>() {{
        add("sink.semantic");
        add("sink.parallelism");
    }};


    /**
     *
     */
    Set<String> kaOptions = new HashSet<String>() {{
        add("topic");
        add("format");
        add("topic-pattern");
        add("key.format");
        add("key.fields");
        add("key.fields-prefix");
        add("value.format");
        add("value.fields-include");
        add("scan.startup.mode");
        add("scan.startup.specific-offsets");
        add("scan.startup.timestamp-millis");
        add("scan.topic-partition-discovery.interval");
        add("json.ignore-parse-errors");
    }};

    private final static String speHead = "properties.";

    public KafkaConnectorConstructImp() {
        super.flinkOptions.addAll(flinkOnlyMeta);
        super.flinkOptions.addAll(kaOptions);
    }

    @Override
    public List<PojoCatalogTableConnInfo> constract(List<PojoCatalogTableConnInfo> sou) {
        return super.addHead(sou, this.flinkOptions, speHead);
    }
}
