import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.junit.Test;
import window.Metric;
import window.Rule;
import window.UserWindow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Cep implements Serializable {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public static TypeInformation<Rule> RULE_TYPE_INF = Types.POJO(Rule.class, new HashMap<String, TypeInformation<?>>() {{
        put("ruleId", Types.INT);
        put("ruleState", TypeInformation.of(Rule.RuleState.class));
        put("groupingKeyNames", Types.LIST(Types.STRING));
        put("aggregateFieldName", Types.STRING);
        put("aggregatorFunctionType", TypeInformation.of(Rule.AggregatorFunctionType.class));
        put("limitOperatorType", TypeInformation.of(Rule.LimitOperatorType.class));
        put("limit", Types.BIG_DEC);
        put("windowMinutes", Types.INT);
    }});
    public static TypeInformation<Metric> METRIC_TYPE_INF = Types.POJO(Metric.class, new HashMap<String, TypeInformation<?>>() {{
        put("tags", Types.MAP(Types.STRING, Types.STRING));
        put("metrics", Types.MAP(Types.STRING, Types.BIG_DEC));
        put("eventTime", Types.LONG);
    }});

    public static final MapStateDescriptor<Integer, Rule> RULE_STATE_DESC = new MapStateDescriptor<>("RuleState", Types.INT, RULE_TYPE_INF);


    // ini env
    {
        env.setParallelism(2);
        // disable generic ser
        env.getConfig().disableGenericTypes();
    }

    private static Metric buildEventtime(Metric seed, Long evnetTime) throws JsonProcessingException {
        Metric m = new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(seed), Metric.class);
        m.setEventTime(evnetTime);
        return m;
    }

    public static List<Metric> returnMetricTestSource() throws JsonProcessingException {

        Metric seedA = new Metric();
        seedA.setTags(new HashMap<String, String>() {{
            put("accept", "200");
            put("relocation", "300");
        }});
        seedA.setEventTime(0L);
        seedA.setMetrics(new HashMap<String, BigDecimal>() {{
            put("200", new BigDecimal("1"));
            put("300", new BigDecimal("1"));
        }});

        Metric seedB = new Metric();
        seedB.setTags(new HashMap<String, String>() {{
            put("accept", "200");
            put("forbidden", "400");
        }});
        seedB.setEventTime(0L);
        seedB.setMetrics(new HashMap<String, BigDecimal>() {{
            put("200", new BigDecimal("1"));
            put("400", new BigDecimal("1"));
        }});

        // 深拷贝
        List<Metric> temp = new ArrayList<>();
        for (long a = 0L; a < 100; a++) {
            // 时间粒度为 s
            temp.add(buildEventtime(seedA, a * 10));
//            temp.add(buildEventtime(seedB, a * 5));
        }

        return temp;

    }


    public static List<Rule> returnRuleTestSource() {
        // 激活
        Rule ruleA = new Rule();
        ruleA.setRuleId(1);
        ruleA.setRuleState(Rule.RuleState.ACTIVE);
        ruleA.setGroupingKeyNames(new ArrayList<String>() {{
            add("accept");
            add("relocation");
        }});
        ruleA.setAggregateFieldName("200");
        ruleA.setLimitOperatorType(Rule.LimitOperatorType.GREATER);
        ruleA.setLimit(new BigDecimal(0));
        ruleA.setWindowMinutes(5);
        ruleA.setAggregatorFunctionType(Rule.AggregatorFunctionType.SUM);


        Function<Rule, Rule> function = (a) -> {
            try {
                return new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(a), Rule.class);
            } catch (JsonProcessingException e) {
            }
            return null;
        };


        // 深拷贝

        return new ArrayList<Rule>() {{
            add(function.apply(ruleA));
        }};
    }


    @Test
    public void testDemo() throws Exception {

        // 构造广播流，广播流里面的配置会自动根据 rule 里面的规则移除，更新规则，按照 rule_id 作为唯一标识进行增删改查
        BroadcastStream<Rule> streamRule = this.env.addSource(new RichSourceFunction<Rule>() {
            @Override
            public void run(SourceContext<Rule> ctx) {
                returnRuleTestSource().forEach(e -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException interruptedException) {

                    }
                    ctx.collect(e);
                });
            }

            @Override
            public void cancel() {

            }
        }).returns(RULE_TYPE_INF).setParallelism(1)
                .returns(RULE_TYPE_INF).broadcast(RULE_STATE_DESC);

        DataStream<Metric> streamRecord = this.env.addSource(new RichSourceFunction<Metric>() {
            @Override
            public void run(SourceContext<Metric> ctx) throws Exception {
                for (Metric each: returnMetricTestSource()){
                    // 模拟数据不定期进入
                    Thread.sleep(RandomUtils.nextInt(10, 500));
                    log.debug(each.toString());
                    ctx.collect(each);
                }

                // 用于演示，用于关闭最后一个窗口
                Thread.sleep(60000);
                Metric seedA = new Metric();
                seedA.setTags(new HashMap<String, String>() {{
                    put("accept", "200");
                    put("relocation", "300");
                }});
                seedA.setEventTime(0L);
                seedA.setMetrics(new HashMap<String, BigDecimal>() {{
                    put("200", new BigDecimal("1"));
                    put("300", new BigDecimal("1"));
                }});
                ctx.collect(seedA);
                ctx.close();
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(1).returns(METRIC_TYPE_INF)
                // 没有说明分组方式，直接拿所有 tag 分组
                .keyBy((KeySelector<Metric, String>) value -> value.getTags().values().stream().sorted().collect(Collectors.joining()));

        streamRecord.connect(streamRule)
                .process(new UserWindow()).print();

        env.execute();

    }

}
