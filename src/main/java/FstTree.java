import java.util.*;
import java.util.stream.Collectors;

/**
 * Fst 树非严谨实现，只有前缀共享，没有后缀共享的 Fst 树。 带有词频出现统计功能；
 * 这里只考虑匹配和纠错相关的问题。如何提示提示， 什么时候激活提示，由上层调用方解决，这里不解决业务问题。
 */
public class FstTree {
    TreeNode readFromHead = new TreeNode(' ');
    TreeNode readFromTail = new TreeNode(' ');

    class Single {
        public Integer loc = 0;
    }

    /**
     * 用于初始化字典树
     *
     * @param word 提示词
     * @param cou  词出现频率
     */
    public void initSearchTree(String word, int cou) {
        this.buildTree(word, this.readFromHead, cou);
        this.buildTree(new StringBuffer(word).reverse().toString(), this.readFromTail, cou);
    }

    public void buildTree(String word, TreeNode buildWay, int cou) {
        int loc = 0;
        Map<Character, TreeNode> nowStep = buildWay.getNext();

        TreeNode preNode = null;
        while (loc < word.length()) {
            Character nowChar = word.charAt(loc);
            if (!nowStep.containsKey(nowChar)) {
                TreeNode temp = new TreeNode(nowChar);
                nowStep.put(nowChar, temp);
            }
            preNode = nowStep.get(nowChar);
            nowStep = nowStep.get(nowChar).getNext();
            loc += 1;
        }
        assert preNode != null;
        preNode.setStop();
        preNode.setCou(cou);
    }

    private List<TreeNode> getMaybeNodeList(String word, TreeNode searchWay, FstTree.Single breakLoc) {
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

    private void getDFSWord(List<WordWithFrequency> returnSource, String buffer, TreeNode now) {

        // 要么递归到底， 要么已经是一个完整的字符就返回值
        if (now.getNext().size() == 0 || now.isStop()) {
            returnSource.add(new WordWithFrequency(buffer + now.getStep(), now.getCou()));
        } else {
            now.getNext().values().forEach(each -> this.getDFSWord(returnSource, buffer + now.getStep(), each));
        }
    }

    private SortedSet<WordWithFrequency> tryComplicate(String word, TreeNode tree, Single passLength) {
        List<WordWithFrequency> temp = new ArrayList<>();
        List<WordWithFrequency> tempNPreview = new ArrayList<>();
        SortedSet<WordWithFrequency> returnSource;
        FstTree.Single breLoc = new FstTree.Single();
        this.getMaybeNodeList(word, tree, breLoc).forEach(each -> this.getDFSWord(temp, "", each));
        returnSource = temp
                .stream()
                .map(e -> new WordWithFrequency(word.substring(0, breLoc.loc) + e.word, e.cou))
                .collect(Collectors.toCollection(TreeSet::new));

        // 当 FST 出现前缀不能完全匹配时，如 : sela 用户可能是想输入 sele ，FstTree 中是没有 sela 的。
        // 尝试进行纠错。纠错逻辑为搜索合法前缀搜索，即搜索 sel。
        // 考虑到用户输入最后一个词，虽然匹配上，有可能是错的，在出现非法匹配时候，去掉一个词，尝试获得更多召回 。
        // 如：frem , fre 是能够匹配到的前缀，这种最后一个输入错误的，都可以获得正确提示。（类似搜索增加召回，获取更多目标值的思想）
        //
        // if 条件大于1防止越界
        if (breLoc.loc < word.length() && breLoc.loc > 1) {
            this.getMaybeNodeList(word.substring(0, breLoc.loc - 1), tree, breLoc)
                    .forEach(each -> this.getDFSWord(tempNPreview, "", each)
                    );
            // 注意，由于用了可变变量，这里 breloc 已经被 - 1
            returnSource.addAll(tempNPreview
                    .stream()
                    .map(e -> new WordWithFrequency(word.substring(0, breLoc.loc) + e.word, e.cou))
                    .collect(Collectors.toList()));
        }
        // 返回最后一次成功的匹配长度，用于上次衡量该次匹配的正确性
        passLength.loc = breLoc.loc;
        return returnSource;
    }

    /**
     * 返回可能词，带有前缀匹配和简单的纠错功能。返回的词符合以下规则：
     * - 不考虑纠错情况下
     * 1. 输入能完全前缀匹配； 如: selec ,返回如： select selexxx ; select 属于高频词排在前
     * 2. 输入 s, 返回所有 s 开头的潜在词。高频词排在前
     * <p>
     * - 错误的定义(能够被纠错的前提是前序，倒序遍历的必须有一个正确的起始值)
     * 1. 假定用户输入的第一个词是正确的， 如 : seleot ，soloct， so 这种, 能够纠错。
     * 2. 对于 aelect, eelect, aect，oeleot  这种，后续是一个正确的起始，能够纠错。
     * 3. 对于 oelece 这种，无法正确纠错。
     * <p>
     * - 如何纠错
     * 1. 前序，倒序遍历，求差集的结果加上+最大的可能前缀匹配+最大可能前缀匹配-1的匹配结果
     *
     * @param word 需要联想或者是纠错的词
     * @return 返回一个潜在词
     */
    public List<String> getComplicate(String word) {

        word = word.toLowerCase();
        Single serarchFromHeadPassLength = new Single();
        Single serarchFromReversePassLength = new Single();

        SortedSet<WordWithFrequency> head = tryComplicate(word, this.readFromHead, serarchFromHeadPassLength);

        // 倒序搜索用于纠错。普通场景没有意义，比如 sel ，倒序会返回 l 倒序的数据(因为没有 les 的字符)
        SortedSet<WordWithFrequency> tail = tryComplicate(
                new StringBuffer(word).reverse().toString(), this.readFromTail, serarchFromReversePassLength)
                .stream()
                .map(e -> new WordWithFrequency(new StringBuffer(e.word).reverse().toString(), e.cou))
                .collect(Collectors.toCollection(TreeSet::new));

        // temp 为交集，优先交集数据排前
        SortedSet<WordWithFrequency> temp = new TreeSet<>(head);
        temp.retainAll(tail);

        // temp 为交集，优先交集数据排前,实现纠错在前
        SortedSet<WordWithFrequency> returnSource = new TreeSet<>(temp);
        // 对比返回匹配的长度，防止前面输入错误，导致纠错错误，如 aelect , 不应该联想 axxx 应该按照后续 elect 纠错
        if (serarchFromReversePassLength.loc > serarchFromHeadPassLength.loc) returnSource.addAll(tail);

        returnSource.addAll(head);

        return returnSource.stream().map(e -> e.word).collect(Collectors.toList());
    }


}

class TreeNode {
    private final Character step;
    private boolean stop = false;
    private int cou = 0;
    private final Map<Character, TreeNode> next;

    /**
     * 根节点必须初始化为 ' ' , 否则只能有一个字符作为起始节点。
     *
     * @param step 当前节点的字符
     */
    public TreeNode(Character step) {
        this.step = step;
        this.next = new HashMap<>();
    }


    public void setCou(int cou) {
        this.cou += cou;
    }

    public void setStop() {
        this.stop = true;
    }

    public int getCou() {
        return cou;
    }

    public boolean isStop() {
        return stop;
    }

    public Character getStep() {
        return step;
    }

    public Map<Character, TreeNode> getNext() {
        return next;
    }
}