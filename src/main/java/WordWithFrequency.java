class WordWithFrequency implements Comparable<WordWithFrequency> {
    String word;
    Integer cou;

    public WordWithFrequency(String word, int cou) {
        this.word = word;
        this.cou = cou;
    }


    @Override
    public int compareTo(WordWithFrequency o) {
        int num = this.cou - o.cou;

        // * -1 是为了升序输出
        if(num==0){
            //这步非常关键，没有判定.计数相同名字不同 ，那set集合就默认是相同元素，就会被覆盖掉
            return this.word.compareTo(o.word)*-1;
        }else {
            return num*-1;
        }
    }
}
