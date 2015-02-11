import java.io.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by Colezea on 19/11/2014.
 */
public class MapReduce {
    // rezultate map
    public static HashMap<String, Vector<HashMap<String, Integer>>> mapResults;

    // rezultate reduce
    public static HashMap<String, HashMap<String, Integer>> reduceResults;

    // rezultate compare
    public static HashMap<String, BigDecimal> compareResults;

    public static void main(String args[]) throws IOException, InterruptedException {
        int nrThreads = Integer.parseInt(args[0]);
        String inputFile = args[1];
        String outputFile = args[2];

        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        final int D = Integer.parseInt(br.readLine()); // nr octeti
        final double X = Double.parseDouble(br.readLine()); // limita similaritate
        final int ND = Integer.parseInt(br.readLine()); // numar documente

        // numele documentelor
        String[] documents = new String[ND];

        //workpool
        WorkPool workPoolMap = new WorkPool(nrThreads);
        WorkPool workPoolReduce = new WorkPool(nrThreads);
        WorkPool workPoolCompare = new WorkPool(nrThreads);

        mapResults = new HashMap<String, Vector<HashMap<String, Integer>>>();
        reduceResults = new HashMap<String, HashMap<String, Integer>>();
        compareResults = new HashMap<String, BigDecimal>();

        // citire documente
        for (int i = 0 ; i < ND; i++){
            documents[i] = br.readLine();
        }

        /*
        Generare si executie task-uri map
         */
        for (int i = 0; i < ND; i++){
            File file = new File(documents[i]);
            long dimension = file.length();
            long index = 0;
            while (index < dimension){
                int actualDim = (int)(((index + D) <= dimension) ? D : (dimension - index));
                workPoolMap.putWork(new PartialSolutionMap(documents[i], actualDim, index));
                index += D;
            }
            mapResults.put(documents[i], new Vector<HashMap<String, Integer>>());
        }

        // creare workeri
        Worker[] workers = new Worker[nrThreads];
        for (int i = 0; i < nrThreads; i++){
            workers[i] = new Worker(workPoolMap);
        }
        // pornire thread-uri
        for (int i = 0; i < nrThreads; i++){
            workers[i].start();
        }
        // asteptare terminare task-uri Map
        for (int i = 0;i < nrThreads; i++){
            workers[i].join();
        }

        /*
        Generare si executare task-uri reduce
         */
        for (int i = 0; i < ND; i++){
            workPoolReduce.putWork(new PartialSolutionReduce(documents[i], mapResults.get(documents[i])));
            reduceResults.put(documents[i], new HashMap<String, Integer>());
        }
        // creare workeri
        workers = new Worker[nrThreads];
        for (int i = 0; i < nrThreads; i++){
            workers[i] = new Worker(workPoolReduce);
        }
        // pornire thread-uri
        for (int i = 0; i < nrThreads; i++){
            workers[i].start();
        }
        // asteptare terminare task-uri Reduce
        for (int i = 0;i < nrThreads; i++){
            workers[i].join();
        }

        /*
        Generare si executare task-uri compare
         */
        for (int i = 0; i < ND; i++){
            for (int j = i+1; j < ND; j++){
                workPoolCompare.putWork(new PartialSolutionCompare(documents[i], documents[j],
                        reduceResults.get(documents[i]), reduceResults.get(documents[j])));

            }
        }
        // creare workeri
        workers = new Worker[nrThreads];
        for (int i = 0; i < nrThreads; i++){
            workers[i] = new Worker(workPoolCompare);
        }
        // pornire thread-uri
        for (int i = 0; i < nrThreads; i++){
            workers[i].start();
        }
        // asteptare terminare task-uri Compare
        for (int i = 0;i < nrThreads; i++){
            workers[i].join();
        }

        // sortare valori
        TreeSet<BigDecimal> sortedValues = new TreeSet<BigDecimal>(new Comparator<BigDecimal>() {
            @Override
            public int compare(BigDecimal o1, BigDecimal o2) {
                return (-1) * o1.compareTo(o2);
            }
        });
        ArrayList<String> names = new ArrayList<String>();
        for (Map.Entry<String, BigDecimal> entry:compareResults.entrySet()){
            BigDecimal x = entry.getValue();
            sortedValues.add(x);
            if (sortedValues.first().equals(x)){
                names.add(0, entry.getKey());
            }
            else {
                int i = 0;
                for (BigDecimal b : sortedValues){
                    if (b.compareTo(x) > 0){
                        i++;
                    }
                }
                names.add(i, entry.getKey());
            }
        }


        // scrierea rezultatelor in fisier
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        BigDecimal XB = new BigDecimal(X);
        int i = 0;
        String l = new String();
        for (BigDecimal b: sortedValues){
            if (b.compareTo(XB)>0){
                l = "";
                l += names.get(i) + ";";
                String nr = b.toString();
                nr = nr.substring(0, 6);
                l += nr;
                l += "\n";

                writer.write(l);
            }
            i++;
        }
        writer.close();
    }

}
