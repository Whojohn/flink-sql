package window;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author john
 */
public class UserWindow extends KeyedBroadcastProcessFunction<String, Metric, Rule, String> {

    final static long FIRE_LAG = 10000;
    final MapStateDescriptor<Long, Map<String, Map<String, BigDecimal>>> USER_DEFINE_WINDOWS_STATE = new MapStateDescriptor("WindowState", Types.LONG, Types.MAP(Types.STRING, Types.LIST(METRIC_TYPE_INF)));
    MapState<Long, Map<String, Map<String, BigDecimal>>> windowsStata;
    final TreeSet<Long> clockTree = new TreeSet<>();

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


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        windowsStata = getRuntimeContext().getMapState(USER_DEFINE_WINDOWS_STATE);
    }

    /**
     * 窗口插入函数，维护以下
     * 1. 按照最小时间粒度分钟，预聚合数据
     * 2. 维护时间粒度列表(treeset)
     *
     * @param value 采集数据
     * @throws Exception 上抛异常
     */
    private void maintainWindows(Metric value) throws Exception {
        // 分钟级预聚合，因为窗口都为分钟级别
        Long minStartEdge = (value.getEventTime() / 60) * 60;
        clockTree.add(minStartEdge);


        String ruleTags = value.getTags().keySet().stream().sorted().collect(Collectors.joining());
        if (!this.windowsStata.contains(minStartEdge)) {
            this.windowsStata.put(minStartEdge, new HashMap<>(16));
        }

        Map<String, Map<String, BigDecimal>> inside = this.windowsStata.get(minStartEdge);
        Map<String, BigDecimal> aggElement = inside.getOrDefault(ruleTags, new HashMap<>(16));
        // "SUM", "MIN", "MAX", "COUNT"
        Map<String, BigDecimal> metrics = value.getMetrics();

        for (String each : metrics.keySet()) {
            String sumMetricName = "SUM" + each;
            String minMetricName = "MIN" + each;
            String maxMetricName = "MAX" + each;
            String countMetricName = "COUNT" + each;
            aggElement.put(sumMetricName, aggElement.getOrDefault(sumMetricName, new BigDecimal("0")).add(value.getMetric(each)));
            aggElement.put(countMetricName, aggElement.getOrDefault(countMetricName, new BigDecimal("0")).add(new BigDecimal("1")));

            if (!aggElement.containsKey(minMetricName)) {
                aggElement.put(minMetricName, value.getMetric(each));
            } else {
                aggElement.put(minMetricName, value.getMetric(each).min(aggElement.get(minMetricName)));

            }

            if (!aggElement.containsKey(maxMetricName)) {
                aggElement.put(maxMetricName, value.getMetric(each));
            } else {
                aggElement.put(maxMetricName, value.getMetric(each).max(aggElement.get(maxMetricName)));
            }

        }
        inside.put(ruleTags, aggElement);
    }

    private void windowsFire(ReadOnlyContext ctx, Collector<String> out) throws Exception {

        ReadOnlyBroadcastState<Integer, Rule> state = ctx.getBroadcastState(RULE_STATE_DESC);
        for (Map.Entry<Integer, Rule> each : state.immutableEntries()) {
            Rule fireRule = each.getValue();
            List<List<Long>> fireList = new ArrayList<>();

            // 取每一个窗口二次聚合粒度，可能时间不连续，比如10分钟聚合 首次时间(01~11 范围内只有) 01 03 07
            if (fireRule.getRuleState().equals(Rule.RuleState.ACTIVE)) {
                long head = clockTree.first();
                long tail = clockTree.last();
                int windowsSize = fireRule.getWindowMinutes();
                long step = head;

                while (step <= tail) {
                    List<Long> temp = new ArrayList<>();
                    long label = step;
                    for (long nowClock : clockTree.tailSet(step)) {
                        if (nowClock < step + windowsSize * 60) {
                            temp.add(nowClock);
                        } else {
                            step = nowClock;
                            break;
                        }
                        label = nowClock;
                    }
                    fireList.add(temp);
                    if (label == tail) {
                        break;
                    }
                }

                // 窗口范围已经确定，计算
                String fetchAgg = fireRule.getGroupingKeyNames().stream().sorted().collect(Collectors.joining());
                Rule.AggregatorFunctionType fireAggType = fireRule.getAggregatorFunctionType();
                if (!fireAggType.equals(Rule.AggregatorFunctionType.AVG)) {
                    String aggName = fireAggType.toString() + fireRule.getAggregateFieldName();
                    for (List<Long> fireWindow : fireList) {


                        List<BigDecimal> aggElement = new ArrayList<>();
                        for (Long lag : fireWindow) {
                            if (this.windowsStata.get(lag).containsKey(fetchAgg)) {
                                aggElement.add(this.windowsStata.get(lag).get(fetchAgg).get(aggName));
                            }
                        }

                        BigDecimal returnSoruce = null;
                        // 没有数据
                        if (aggElement.size() < 1) {
                            return;
                        }

                        if (fireAggType.equals(Rule.AggregatorFunctionType.SUM) ||
                                fireAggType.equals(Rule.AggregatorFunctionType.COUNT)) {
                            returnSoruce = aggElement.stream().reduce(BigDecimal::add).get();
                        } else if (fireAggType.equals(Rule.AggregatorFunctionType.MAX)) {
                            returnSoruce = aggElement.stream().reduce(BigDecimal::max).get();
                        } else if (fireAggType.equals(Rule.AggregatorFunctionType.MIN)) {
                            returnSoruce = aggElement.stream().reduce(BigDecimal::min).get();
                        }

                        // cep
                        if (fireRule.apply(returnSoruce)) {
                            // fire the window
                            out.collect(fireRule.toString() + "  Windows start time:" + fireWindow.get(0) +
                                    "  Windows end time:" + fireWindow.get(fireWindow.size()-1) +
                                    " Windows result:" + returnSoruce);
                        }

                    }
                } else {
                    // avg 逻辑 = sum()/count() 类似的。。。懒得写了
                }


            }


        }
        // 清空 窗口变量
        this.windowsStata.clear();
        // 清空时钟
        this.clockTree.clear();
        // 更新 process time 时钟


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // cep 输出控制
        // 实现效果类似 processtime 窗口，每60s 触发一次窗口。
        // 窗口聚合粒度是 tumble window，与 flink 实现区别是起始时间不以自然时间为起始，比如 windowMinutes 为10，
        // flink 会取 00:00 00:10 00:20 这样去聚合。
        // 这里只是简单的以最早出现时间如： 00:03 00:13 00:23 进行聚合
        this.windowsFire(ctx, out);

    }

    @Override
    public void processElement(Metric value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // 每一条数据都会进入到窗口中累记录
        this.maintainWindows(value);
        // 定期输出窗口
        ctx.timerService().registerProcessingTimeTimer(((System.currentTimeMillis() / FIRE_LAG) + 1) * FIRE_LAG);
    }

    @Override
    public void close() throws Exception {

        super.close();
    }

    @Override
    public void processBroadcastElement(Rule value, Context ctx, Collector<String> out) throws Exception {
        BroadcastState<Integer, Rule> state = ctx.getBroadcastState(RULE_STATE_DESC);

        Integer id = value.getRuleId();
        Rule temp = state.get(id);
        if (temp == null) {
            state.put(id, value);
            temp = state.get(id);
        }


        // 规则处理逻辑
        if (value.getRuleState().equals(Rule.RuleState.DELETE)) {
            state.remove(id);
        } else if ((value.getRuleState().equals(Rule.RuleState.PAUSE)) || (value.getRuleState().equals(Rule.RuleState.ACTIVE))) {
            temp.setRuleState(value.getRuleState());
            state.put(id, temp);
        } else if (value.getRuleState().equals(Rule.RuleState.UPDATE)) {
            // 非空部分的值更新
            temp.setRuleId(value.getRuleId() == null ? temp.getRuleId() : value.getRuleId());
            temp.setRuleState(value.getRuleState() == null ? temp.getRuleState() : value.getRuleState());
            temp.setGroupingKeyNames(value.getGroupingKeyNames() == null ? temp.getGroupingKeyNames() : value.getGroupingKeyNames());
            temp.setAggregateFieldName(value.getAggregateFieldName() == null ? temp.getAggregateFieldName() : value.getAggregateFieldName());
            temp.setLimitOperatorType(value.getLimitOperatorType() == null ? temp.getLimitOperatorType() : value.getLimitOperatorType());
            temp.setLimit(value.getLimit() == null ? temp.getLimit() : value.getLimit());
            temp.setWindowMinutes(value.getWindowMinutes() == null ? temp.getWindowMinutes() : value.getWindowMinutes());
            temp.setAggregatorFunctionType(value.getAggregatorFunctionType() == null ? temp.getAggregatorFunctionType() : value.getAggregatorFunctionType());
        }

    }
}
