package pa2.lemma;

import utils.FileUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Tokenizer {
    private Set<String> stopWords;
    private Set<String> convertFormat;


    public Tokenizer(String stopwordPath, String convertFormatPath) {
        stopWords = FileUtil.convertLines2Set(stopwordPath);
        convertFormat = FileUtil.convertLines2Set(convertFormatPath);
    }

    public List<String> tokenize(String sentence) {
        //1. to lowcase
        sentence = sentence.toLowerCase();
        //2. split
        List<String> tokens = SpliteUtil.getTokerns(sentence);
        //3.remove
        removeSpecifyItem(tokens);
        removeStopWords(tokens);
        //4.convert

        return tokens;
    }

    private void removeSpecifyItem(List<String> list) {
        //save list
        List<String> list0 = new LinkedList<>(list);
        //remove element
        for (String e0 : list0) {
            if (PatternUtil.mustRemove(e0)) {
                list.remove(e0);
            }
        }
    }

    private void removeStopWords(List<String> list) {
        //save list
        List<String> list0 = new LinkedList<>(list);
        //remove element
        for (String e0 : list0) {
            if (stopWords.contains(e0)) {
                list.remove(e0);
            }
        }
    }
}
