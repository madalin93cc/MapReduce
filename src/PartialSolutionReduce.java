import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * PartialSolutionReduce.java
 * Created by Madalin.Colezea on 11/21/2014.
 */
public class PartialSolutionReduce implements PartialSolution {

    private String numeFisier;
    private Vector<HashMap<String, Integer>> partialSolutionMap;

    private HashMap<String, Integer> partialSolutionReduce;

    public PartialSolutionReduce(String numeFisier, Vector<HashMap<String, Integer>> partialSolutionMap) {
        this.numeFisier = numeFisier;
        this.partialSolutionMap = partialSolutionMap;

        partialSolutionReduce = new HashMap<String, Integer>();
    }

    @Override
    public void executePartialSolution() {
        for (HashMap<String, Integer> map : partialSolutionMap){
            for (Map.Entry<String, Integer> entry:map.entrySet()){
                Integer nr = partialSolutionReduce.get(entry.getKey());
                if (entry.getKey().equals("holding") && (numeFisier.equals("1mb-1"))){
                }
                if (nr == null){
                    nr = 0;
                }
                nr += entry.getValue();
                partialSolutionReduce.put(entry.getKey(), nr);
            }
        }
        MapReduce.reduceResults.put(numeFisier, partialSolutionReduce);
    }
}
