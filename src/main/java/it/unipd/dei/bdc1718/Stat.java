package it.unipd.dei.bdc1718;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import static it.unipd.dei.bdc1718.G21HM3ElFurlo.kMeansObj;
import static it.unipd.dei.bdc1718.G21HM3ElFurlo.kMeansPP;


public class Stat{
    
    public static void main(String []args) throws IOException{
        ArrayList<Vector> P = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        ArrayList<Vector> C2 = new ArrayList<Vector>();
        ArrayList<Long> WP2 = new ArrayList<Long>();

        ArrayList<Tuple2> risultati = new ArrayList<>();
        for(int i=0; i<P.size();i++){
                        WP2.add(1L);
        }



        int numProve =20;
        int kIter = 20;
        double totalSum=0;
        for(int i=2; i<=kIter; i++){
            double tempSum=0;
            System.out.println("siamo arrivati a "+i+" clusters++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");


            for(int m=0; m<numProve;m++) {
                ArrayList<Vector> Pcopia = (ArrayList<Vector>)P.clone();
                ArrayList<Long> WP2copia = (ArrayList<Long>) WP2.clone();

                C2=kMeansPP(Pcopia,WP2copia,i);

                double result2 = kMeansObj(Pcopia, C2);
                tempSum += result2;
            }

            double average = (tempSum/numProve);
            Tuple2 ciccio = new Tuple2(i,average);
            risultati.add(ciccio);
        }

        System.out.println("la dimensioe di p alla fine è = "+ P.size());
        for (Tuple2 pippo: risultati) {
            System.out.println("NumCluster,Media := " + pippo);
        }


    }
}