import java.util.*;
import java.util.stream.Collectors;

public class SqlAutoComplete {
    private final FstTree fstTree = new FstTree();

    {
        // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sql/overview/#reserved-keywords
        String flinkReservedWord = "A, ABS, ABSOLUTE, ACTION, ADA, ADD, ADMIN, AFTER, ALL, ALLOCATE, ALLOW, ALTER, ALWAYS, AND, ANY, ARE, ARRAY, AS, ASC, ASENSITIVE, ASSERTION, ASSIGNMENT, ASYMMETRIC, AT, ATOMIC, ATTRIBUTE, ATTRIBUTES, AUTHORIZATION, AVG, BEFORE, BEGIN, BERNOULLI, BETWEEN, BIGINT, BINARY, BIT, BLOB, BOOLEAN, BOTH, BREADTH, BY, BYTES, C, CALL, CALLED, CARDINALITY, CASCADE, CASCADED, CASE, CAST, CATALOG, CATALOG_NAME, CEIL, CEILING, CENTURY, CHAIN, CHAR, CHARACTER, CHARACTERISTICS, CHARACTERS, CHARACTER_LENGTH, CHARACTER_SET_CATALOG, CHARACTER_SET_NAME, CHARACTER_SET_SCHEMA, CHAR_LENGTH, CHECK, CLASS_ORIGIN, CLOB, CLOSE, COALESCE, COBOL, COLLATE, COLLATION, COLLATION_CATALOG, COLLATION_NAME, COLLATION_SCHEMA, COLLECT, COLUMN, COLUMNS, COLUMN_NAME, COMMAND_FUNCTION, COMMAND_FUNCTION_CODE, COMMIT, COMMITTED, CONDITION, CONDITION_NUMBER, CONNECT, CONNECTION, CONNECTION_NAME, CONSTRAINT, CONSTRAINTS, CONSTRAINT_CATALOG, CONSTRAINT_NAME, CONSTRAINT_SCHEMA, CONSTRUCTOR, CONTAINS, CONTINUE, CONVERT, CORR, CORRESPONDING, COUNT, COVAR_POP, COVAR_SAMP, CREATE, CROSS, CUBE, CUME_DIST, CURRENT, CURRENT_CATALOG, CURRENT_DATE, CURRENT_DEFAULT_TRANSFORM_GROUP, CURRENT_PATH, CURRENT_ROLE, CURRENT_SCHEMA, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TRANSFORM_GROUP_FOR_TYPE, CURRENT_USER, CURSOR, CURSOR_NAME, CYCLE, DATA, DATABASE, DATE, DATETIME_INTERVAL_CODE, DATETIME_INTERVAL_PRECISION, DAY, DEALLOCATE, DEC, DECADE, DECIMAL, DECLARE, DEFAULT, DEFAULTS, DEFERRABLE, DEFERRED, DEFINED, DEFINER, DEGREE, DELETE, DENSE_RANK, DEPTH, DEREF, DERIVED, DESC, DESCRIBE, DESCRIPTION, DESCRIPTOR, DETERMINISTIC, DIAGNOSTICS, DISALLOW, DISCONNECT, DISPATCH, DISTINCT, DOMAIN, DOUBLE, DOW, DOY, DROP, DYNAMIC, DYNAMIC_FUNCTION, DYNAMIC_FUNCTION_CODE, EACH, ELEMENT, ELSE, END, END-EXEC, EPOCH, EQUALS, ESCAPE, EVERY, EXCEPT, EXCEPTION, EXCLUDE, EXCLUDING, EXEC, EXECUTE, EXISTS, EXP, EXPLAIN, EXTEND, EXTERNAL, EXTRACT, FALSE, FETCH, FILTER, FINAL, FIRST, FIRST_VALUE, FLOAT, FLOOR, FOLLOWING, FOR, FOREIGN, FORTRAN, FOUND, FRAC_SECOND, FREE, FROM, FULL, FUNCTION, FUSION, G, GENERAL, GENERATED, GET, GLOBAL, GO, GOTO, GRANT, GRANTED, GROUP, GROUPING, HAVING, HIERARCHY, HOLD, HOUR, IDENTITY, IMMEDIATE, IMPLEMENTATION, IMPORT, IN, INCLUDING, INCREMENT, INDICATOR, INITIALLY, INNER, INOUT, INPUT, INSENSITIVE, INSERT, INSTANCE, INSTANTIABLE, INT, INTEGER, INTERSECT, INTERSECTION, INTERVAL, INTO, INVOKER, IS, ISOLATION, JAVA, JOIN, K, KEY, KEY_MEMBER, KEY_TYPE, LABEL, LANGUAGE, LARGE, LAST, LAST_VALUE, LATERAL, LEADING, LEFT, LENGTH, LEVEL, LIBRARY, LIKE, LIMIT, LN, LOCAL, LOCALTIME, LOCALTIMESTAMP, LOCATOR, LOWER, M, MAP, MATCH, MATCHED, MAX, MAXVALUE, MEMBER, MERGE, MESSAGE_LENGTH, MESSAGE_OCTET_LENGTH, MESSAGE_TEXT, METHOD, MICROSECOND, MILLENNIUM, MIN, MINUTE, MINVALUE, MOD, MODIFIES, MODULE, MODULES, MONTH, MORE, MULTISET, MUMPS, NAME, NAMES, NATIONAL, NATURAL, NCHAR, NCLOB, NESTING, NEW, NEXT, NO, NONE, NORMALIZE, NORMALIZED, NOT, NULL, NULLABLE, NULLIF, NULLS, NUMBER, NUMERIC, OBJECT, OCTETS, OCTET_LENGTH, OF, OFFSET, OLD, ON, ONLY, OPEN, OPTION, OPTIONS, OR, ORDER, ORDERING, ORDINALITY, OTHERS, OUT, OUTER, OUTPUT, OVER, OVERLAPS, OVERLAY, OVERRIDING, PAD, PARAMETER, PARAMETER_MODE, PARAMETER_NAME, PARAMETER_ORDINAL_POSITION, PARAMETER_SPECIFIC_CATALOG, PARAMETER_SPECIFIC_NAME, PARAMETER_SPECIFIC_SCHEMA, PARTIAL, PARTITION, PASCAL, PASSTHROUGH, PATH, PERCENTILE_CONT, PERCENTILE_DISC, PERCENT_RANK, PLACING, PLAN, PLI, POSITION, POWER, PRECEDING, PRECISION, PREPARE, PRESERVE, PRIMARY, PRIOR, PRIVILEGES, PROCEDURE, PUBLIC, QUARTER, RANGE, RANK, RAW, READ, READS, REAL, RECURSIVE, REF, REFERENCES, REFERENCING, REGR_AVGX, REGR_AVGY, REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX, REGR_SXY, REGR_SYY, RELATIVE, RELEASE, REPEATABLE, RESET, RESTART, RESTRICT, RESULT, RETURN, RETURNED_CARDINALITY, RETURNED_LENGTH, RETURNED_OCTET_LENGTH, RETURNED_SQLSTATE, RETURNS, REVOKE, RIGHT, ROLE, ROLLBACK, ROLLUP, ROUTINE, ROUTINE_CATALOG, ROUTINE_NAME, ROUTINE_SCHEMA, ROW, ROWS, ROW_COUNT, ROW_NUMBER, SAVEPOINT, SCALE, SCHEMA, SCHEMA_NAME, SCOPE, SCOPE_CATALOGS, SCOPE_NAME, SCOPE_SCHEMA, SCROLL, SEARCH, SECOND, SECTION, SECURITY, SELECT, SELF, SENSITIVE, SEQUENCE, SERIALIZABLE, SERVER, SERVER_NAME, SESSION, SESSION_USER, SET, SETS, SIMILAR, SIMPLE, SIZE, SMALLINT, SOME, SOURCE, SPACE, SPECIFIC, SPECIFICTYPE, SPECIFIC_NAME, SQL, SQLEXCEPTION, SQLSTATE, SQLWARNING, SQL_TSI_DAY, SQL_TSI_FRAC_SECOND, SQL_TSI_HOUR, SQL_TSI_MICROSECOND, SQL_TSI_MINUTE, SQL_TSI_MONTH, SQL_TSI_QUARTER, SQL_TSI_SECOND, SQL_TSI_WEEK, SQL_TSI_YEAR, SQRT, START, STATE, STATEMENT, STATIC, STDDEV_POP, STDDEV_SAMP, STREAM, STRING, STRUCTURE, STYLE, SUBCLASS_ORIGIN, SUBMULTISET, SUBSTITUTE, SUBSTRING, SUM, SYMMETRIC, SYSTEM, SYSTEM_USER, TABLE, TABLESAMPLE, TABLE_NAME, TEMPORARY, THEN, TIES, TIME, TIMESTAMP, TIMESTAMPADD, TIMESTAMPDIFF, TIMEZONE_HOUR, TIMEZONE_MINUTE, TINYINT, TO, TOP_LEVEL_COUNT, TRAILING, TRANSACTION, TRANSACTIONS_ACTIVE, TRANSACTIONS_COMMITTED, TRANSACTIONS_ROLLED_BACK, TRANSFORM, TRANSFORMS, TRANSLATE, TRANSLATION, TREAT, TRIGGER, TRIGGER_CATALOG, TRIGGER_NAME, TRIGGER_SCHEMA, TRIM, TRUE, TYPE, UESCAPE, UNBOUNDED, UNCOMMITTED, UNDER, UNION, UNIQUE, UNKNOWN, UNNAMED, UNNEST, UPDATE, UPPER, UPSERT, USAGE, USER, USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_CODE, USER_DEFINED_TYPE_NAME, USER_DEFINED_TYPE_SCHEMA, USING, VALUE, VALUES, VARBINARY, VARCHAR, VARYING, VAR_POP, VAR_SAMP, VERSION, VIEW, WEEK, WHEN, WHENEVER, WHERE, WIDTH_BUCKET, WINDOW, WITH, WITHIN, WITHOUT, WORK, WRAPPER, WRITE, XML, YEAR, ZONE";
        Arrays.stream(flinkReservedWord.split(",")).map(e -> e.trim().toLowerCase()).forEach(
                fstTree::initSearchTree
        );
        String characterNotice = "(),<>,\"\",'',{}";
        Arrays.stream(characterNotice.split(",")).map(e -> e.trim().toLowerCase()).forEach(
                fstTree::initSearchTree
        );
    }

    public List<String> getComplete(String source) {
        // 空格不需要提示
        if (source.length() > 0 && source.charAt(source.length() - 1) == ' ') return new ArrayList<>();
        String[] temp = source.split("\\s");
        return fstTree.getComplicate(temp[temp.length - 1]);
    }
}

class FstTree {
    TreeNode readFromHead = new TreeNode(' ');
    TreeNode readFromTail = new TreeNode(' ');

    class Single {
        public Integer loc = 0;
    }

    public void initSearchTree(String word) {
        this.buildTree(word, this.readFromHead);
        this.buildTree(new StringBuffer(word).reverse().toString(), this.readFromTail);
    }

    public void buildTree(String word, TreeNode buildWay) {
        int loc = 0;

        Map<Character, TreeNode> nowStep = buildWay.getNext();

        while (loc < word.length()) {
            Character nowChar = word.charAt(loc);

            if (!nowStep.containsKey(nowChar)) {
                TreeNode temp = new TreeNode(nowChar);
                nowStep.put(nowChar, temp);
            }
            nowStep = nowStep.get(nowChar).getNext();
            loc += 1;
        }
    }

    public List<TreeNode> getMaybeNodeList(String word, TreeNode searchWay, Single breakLoc) {
        int loc = 0;
        Map<Character, TreeNode> nowStep = searchWay.getNext();
        while (loc < word.length()) {
            Character nowChar = word.charAt(loc);

            if (!nowStep.containsKey(nowChar)) {
                // maybe wrong typing
                breakLoc.loc = loc;
                break;
            }
            nowStep = nowStep.get(nowChar).getNext();
            loc += 1;
        }
        breakLoc.loc = loc;
        // 最起码有1位以上的匹配，不然全部输出没有意义
        return loc > 0 ? new ArrayList<>(nowStep.values()) : new ArrayList<>();
    }

    public void getDFSWord(List<String> returnSource, String buffer, TreeNode now) {


        if (now.getNext().size() == 0) {
            returnSource.add(buffer + now.getStep());
        } else {
            now.getNext().values().forEach(each -> this.getDFSWord(returnSource, buffer + now.getStep(), each));
        }
    }

    public Set<String> tryComplicate(String word, TreeNode tree) {
        List<String> temp = new ArrayList<>();
        Single breLoc = new Single();
        this.getMaybeNodeList(word, tree, breLoc).forEach(each -> this.getDFSWord(temp, "", each));

        //  有可能最后一个可以找到的词为错误词，去掉一个词，尝试获得更多召回 . frem 这种最后一个输入错误的，都可以获得正确提示。
        //  大于1防止越界
        //  类似搜索增加召回 ;
        if (breLoc.loc < word.length() && breLoc.loc > 1) {
            this.getMaybeNodeList(word.substring(0, breLoc.loc - 1), tree, breLoc).forEach(each -> this.getDFSWord(temp, "", each));
        }

        return temp.stream().map(e -> word.substring(0, breLoc.loc) + e).collect(Collectors.toSet());
    }

    public List<String> getComplicate(String word) {
        word = word.toLowerCase();
        List<String> returnSource = new ArrayList<>();

        Set<String> head = tryComplicate(word, this.readFromHead);
        Set<String> tail = tryComplicate(new StringBuffer(word).reverse().toString(), this.readFromTail)
                .stream()
                .map(e -> new StringBuffer(e).reverse().toString())
                .collect(Collectors.toSet());
        Set<String> temp = new HashSet<>(head);
        temp.retainAll(tail);
        head.removeAll(tail);


        returnSource.addAll(temp);
        returnSource.addAll(head);
        return returnSource;
    }


}

class TreeNode {
    private final Character step;
    private final Map<Character, TreeNode> next;

    public TreeNode(Character step) {
        this.step = step;
        this.next = new HashMap<>();
    }

    public Character getStep() {
        return step;
    }

    public Map<Character, TreeNode> getNext() {
        return next;
    }
}

