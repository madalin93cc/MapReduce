import java.io.IOException;

/**
 * Worker.java
 * Created by Madalin.Colezea on 11/21/2014.
 */
public class Worker extends Thread {

    WorkPool workPool;

    public Worker(WorkPool workpool) {
        this.workPool = workpool;
    }

    /**
     * Procesarea unei solutii partiale.
     */
    void processPartialSolution(PartialSolution ps) throws IOException {
        ps.executePartialSolution();
    }

    public void run() {
        while (true) {
            PartialSolution partialSolution = workPool.getWork();
            if (partialSolution == null)
                break;

            try {
                processPartialSolution(partialSolution);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
