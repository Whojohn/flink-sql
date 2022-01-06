package window;

import lombok.Data;
import org.apache.flink.api.common.time.Time;

import java.math.BigDecimal;
import java.util.List;

@Data
public class Rule {

    // ruleId 自增键位？？？ 规则唯一标识？？？
    // 为什么不取 groupingKeyNames 作为唯一标识呢？
    // 假如允许相同 groupingKeyNames 不同 ruleid 那么多规则下，每一轮窗口是按顺序触发？只触发一次，还是多次触发？？？

    // 实现时候，暴力的认为 ruleid 就是规则唯一标识
    // 一个轮次，允许多个 groupingKeyNames 相同, 且触发多规则的发送(一份数据，多个发送), 所有规则判定完成后清理窗口
    private Integer ruleId;
    private RuleState ruleState;
    // Group by {@link Metric#getTag(String)}
    private List<String> groupingKeyNames;
    // Query from {@link Metric#getMetric(String)}

    // 实现时候采用 metric key，假如使用 tags ,可能存在一个 key 对应多个 value 的情况。文档根本没说重叠的情况怎么处理
    // 鉴于根本没示例数据，无法知道业务，直接采用 metirc key
    private String aggregateFieldName;
    private AggregatorFunctionType aggregatorFunctionType;
    private LimitOperatorType limitOperatorType;
    private BigDecimal limit;
    private Integer windowMinutes;

    public Long fetchWindowMillis() {
        return Time.minutes(this.windowMinutes).toMilliseconds();
    }

    public long fetchWindowStartFor(Long timestamp) {
        Long ruleWindowMillis = fetchWindowMillis();
        return (timestamp - ruleWindowMillis);
    }

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator
     * type.*
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }


    public enum AggregatorFunctionType {
        SUM, AVG, MIN, MAX, COUNT
    }

    public enum LimitOperatorType {
        EQUAL("="), NOT_EQUAL("!="), GREATER_EQUAL(">="), LESS_EQUAL("<="), GREATER(">"), LESS("<");
        String operator;

        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        public static LimitOperatorType fromString(String text) {
            for (LimitOperatorType b : LimitOperatorType.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }

    public enum RuleState {
        ACTIVE, PAUSE, DELETE, UPDATE
    }
}