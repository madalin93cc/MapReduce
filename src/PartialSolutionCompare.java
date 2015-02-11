import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;

/**
 * PartialSolutionCompare.java
 * Created by Madalin.Colezea on 11/21/2014.
 */
public class PartialSolutionCompare implements PartialSolution {
    String fileName1;
    String fileName2;
    HashMap<String, Integer> reduceResult1;
    HashMap<String, Integer> reduceResult2;

    public PartialSolutionCompare(String fileName1, String fileName2, HashMap<String, Integer> reduceResult1, HashMap<String, Integer> reduceResult2) {
        this.fileName1 = fileName1;
        this.fileName2 = fileName2;
        this.reduceResult1 = reduceResult1;
        this.reduceResult2 = reduceResult2;
    }

    @Override
    public void executePartialSolution() {
        String files = fileName1 + ";" + fileName2; // cheie
        HashSet<String> words = new HashSet<String>();
        Double nWords1; // numar cuvinte fisier 1
        Double nWords2; // numar cuvinte fisier 2
        Integer nr1, nr2;
        Double f1, f2, sim = 0d;
        BigDecimal similarity = new BigDecimal(0);
        Double n1 = 0d,n2 = 0d;

        for (Integer i : reduceResult1.values()){
            n1 += i;
        }
        for (Integer i : reduceResult2.values()){
            n2 += i;
        }

        nWords1 = new Double(n1);
        nWords2 = new Double(n2);

        words.addAll(reduceResult1.keySet());
        words.addAll(reduceResult2.keySet());
        for (String s : words){
            // numar aparitie
            nr1 = reduceResult1.get(s);
            if (nr1 == null) nr1 = 0;
            nr2 = reduceResult2.get(s);
            if (nr2 == null) nr2 = 0;
            // frecente
            f1 = (nr1 / nWords1) * 100;
            f2 = (nr2 / nWords2) * 100;
            sim += (f1 * f2)/100;
        }
        similarity = new BigDecimal(sim);
        MapReduce.compareResults.put(files, similarity);
    }
}
