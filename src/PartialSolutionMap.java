import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 * PartialSolutionMAP.java
 * Created by Madalin.Colezea on 11/21/2014.
 */
public class PartialSolutionMap implements PartialSolution {

    private String numeFisier;
    private long offsetStart;
    private int dimensiune;

    HashMap<String, Integer> partialResult;

    byte[] text;
    RandomAccessFile file;
    int posI;
    boolean addOk = false;

    String delimitators = new String("_;:/?~\\.,><~`[]{}()!@#$%^&-+'=*|\"\t\n ");

    public PartialSolutionMap(String numeFisier, int dimensiune, long offsetStart) throws FileNotFoundException {
        this.numeFisier = numeFisier;
        this.dimensiune = dimensiune;
        this.offsetStart = offsetStart;

        partialResult = new HashMap<String, Integer>();
    }

    @Override
    public void executePartialSolution() throws IOException {
        file = new RandomAccessFile(numeFisier, "r");
        if (offsetStart == 0){
            text = new byte[dimensiune + 20];
        }
        else {
            text = new byte[dimensiune + 20];
            offsetStart--;
        }
        file.seek(offsetStart);
        file.read(text);
        String s = new String(text);
        file.close();
        posI = 0;
        String word;

        if (offsetStart != 0) {
            if (!isDelimitator(text[0])) {
                while (!isDelimitator(text[posI])) {
                    posI++;
                }
            }
        }
        while (posI < dimensiune) {
            word = "";
            if (offsetStart == 0) { // inceput de fisier
                // ignor delimitatorii
                if (isDelimitator(text[posI])) {
                    while (isDelimitator(text[posI])) {
                        if (posI < dimensiune)
                            posI++;
                        else
                            break;
                    }
                }
                // am ajuns la litera
                while (!isDelimitator(text[posI])) { // cat time nu e delimitator
                    if (posI < dimensiune) {
                        word += (char) text[posI];
                        posI++;
                    } else {
                        addOk = true;
                        while (!isDelimitator(text[posI])){ // s-a depasit dimensiunea dar nu s-a terminat cuvantul
                            word += (char) text[posI];
                            posI++;
                            dimensiune++;
                        }
                    }
                }
                // am ajuns la delimitator
                posI++;
                processWord(word);
            }
            else { // in interiorul fisierului
                // ignor delimitatorii
                if (isDelimitator(text[posI])) {
                    while (isDelimitator(text[posI])) {
                        if (posI < dimensiune)
                            posI++;
                        else
                            break;
                    }
                }
                // am ajuns la litera
                while (!isDelimitator(text[posI])) { // cat timp e litera
                    if (posI < dimensiune) { // daca nu s-a ajuns la dimensiunea maxima
                        word += (char) text[posI];
                        posI++;

                    }
                    else { // am ajuns la final dar nu s-a terminat cuvantul
                        addOk = true;
                        while (!isDelimitator(text[posI])){
                            word += (char) text[posI];
                            posI++;
                            dimensiune++;
                        }
                    }
                }
                // am ajuns la delimitator
                posI++;
                processWord(word);
                // daca pe ultima pozitie incepe un cuvant il procesez
                if ((posI == dimensiune) && (!isDelimitator(text[posI]))){
                    word = "";
                    while (!isDelimitator(text[posI])){
                        word += (char) text[posI];
                        dimensiune++;
                        posI++;
                    }
                    processWord(word);
                }
            }
        }
        if (posI >= dimensiune){
            MapReduce.mapResults.get(numeFisier).add(partialResult);
        }
    }

    /*
    Metoda ce dauga un cuvant la solutia partiala si actualizeaza
    solutia generala a Map-ului
     */
    private void processWord(String word){
        word = word.toLowerCase();
        if (!word.equals("")) {
            Integer actualNr = partialResult.get(word);
            if (actualNr != null) {
                actualNr++;
                partialResult.put(word, actualNr);
            } else {
                actualNr = 1;
                partialResult.put(word, actualNr);
            }
        }
    }

    /*
    Metoda ce verifica daca un caracter este delimitator
     */
    boolean isDelimitator(byte b){
        if (b == 0) return true; // daca e null / EOF
        Character c = (char) b;
        if (delimitators.contains(c.toString()) == true){
            return true;
        }
        return false;
    }

}
