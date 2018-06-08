package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;


public class G21HM3ElFurlo {


    //ElAlaMAIN
    public static void main(String[] args) throws IOException {

        //Scanngrer and variables
        Scanner consoleInput = new Scanner(System.in);

        //time check parameters
        long start = 0;
        long end = 0;

//temporary input
        //put the content of a file in the P Arraylist<Vector>
        //ArrayList<Vector> P = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        //int k = 5;
        //int k1 = 50;
//

        ArrayList<Vector> P = new ArrayList<Vector>();

        //Algorithm choice
        String sChooseAlgo = "CHOOSE AN ALGORITHM:\n" +
                "1 - kCenter.\t" +
                "2 - Weighted 1 kMeans++ + kMeansObj.\t" +
                "3 - kCenter +kMeans++ + kMeansObj.\t" +
                "9 - Exit.";

        //file choice
        String  sChooseFile = "Choose a file; they are dataset of increasing dimensions:\n"+
                "1 - vecs-50-10000.txt\t"+"2 - vecs-50-50000.txt\t"+"3 - vecs-50-100000.txt\t"+"4 - vecs-50-500000.txt\t";

        //Execution loop
        while (true) {
            System.out.println(sChooseAlgo);
            int iChosenAlgo = consoleInput.nextInt();
            System.out.println("Choice number " + iChosenAlgo + ". Ongoing elaboration of input file...");
            switch (iChosenAlgo) {

                case 1:

                    //chosing file,k
                    System.out.println(sChooseFile);
                    P = choosingFile(consoleInput.nextInt());

                    System.out.println("Choose a center set dimension: ");
                    int kMethod1 = consoleInput.nextInt();
                    //end choosing file,k

                    start = System.currentTimeMillis();
                            ArrayList<Vector> centers = kCenter(P,kMethod1);
                    end = System.currentTimeMillis();
                    System.out.println("Kcenter elapsed time " + (end - start) + " ms");
                    System.out.println(" ");
                    System.out.println("****************************************************************************************************");
                    System.out.println("****************************************************************************************************");
                    System.out.println("  ");
                    System.out.println("  ");
                    break;

                case 2:
                    //chosing file,k
                    System.out.println(sChooseFile);
                    P = choosingFile(consoleInput.nextInt());

                    System.out.println("Choose a center set dimension: ");
                    int kMethod2 = consoleInput.nextInt();
                    //end choosefile, k

                    ArrayList<Long> WP2 = new ArrayList<Long>();
                    //filling weigth vector of 1s
                    for(int i=0; i<P.size();i++){
                        WP2.add(1L);
                    }
                    ArrayList<Vector> C2 = new ArrayList<Vector>();
                    start = System.currentTimeMillis();
                    C2=kMeansPP(P,WP2,kMethod2);
                    end = System.currentTimeMillis();
                    System.out.println("Kmeans++ elapsed time " + (end - start) + " ms");
                    double result2 = kMeansObj(P,C2);
                    System.out.println("The average minimal distance between points and centers: "+ result2);
                    System.out.println(" ");
                    System.out.println("****************************************************************************************************");
                    System.out.println("****************************************************************************************************");
                    System.out.println("  ");
                    System.out.println("  ");
                    break;

                case 3:
                    //chosing file,k,k1
                    System.out.println(sChooseFile);
                    P = choosingFile(consoleInput.nextInt());

                    System.out.println("Choose a final center set dimension: ");
                    int kMethod3 = consoleInput.nextInt();
                    System.out.println("Choose a 2 level center set dimension (bigger than the previous): ");
                    int k1 = consoleInput.nextInt();
                    //end of choosing file, k, k1

                    ArrayList<Vector> X3 = new ArrayList<Vector>();
                    ArrayList<Long> WP3 = new ArrayList<Long>();

                    //filling weigth vector of 1s
                    for(int i=0; i<P.size();i++){
                        WP3.add(1L);
                    }
                    ArrayList<Vector> C3 = new ArrayList<Vector>();
                    X3 = kCenter(P,k1);
                    C3 = kMeansPP(X3,WP3,kMethod3);
                    double result3 = kMeansObj(P,C3);
                    System.out.println("The average minimal distance between points and centers: "+ result3);
                    System.out.println(" ");
                    System.out.println("****************************************************************************************************");
                    System.out.println("****************************************************************************************************");
                    System.out.println("  ");
                    System.out.println("  ");
                    break;

                case 9:
                    System.out.println("Exiting...");
                    return;

                default:
                    System.out.println("Invalid algorithm number.");
                    System.out.println(" ");
                    System.out.println("****************************************************************************************************");
                    System.out.println("****************************************************************************************************");
                    System.out.println("  ");
                    System.out.println("  ");
                    break;

            }//end of switch

        }//end of infinite loop

    }//end of main


    //Method Kcenter
    public static ArrayList<Vector> kCenter(ArrayList<Vector> Pinput, int Kinput){

//fanciness
        System.out.println("Applyng the Kcenter Alg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//
        //building C, the set of chosen points
        ArrayList<Vector> C = new ArrayList<Vector>();
        //choosing a random point in the set P
        int totalPointsNumber = Pinput.size();

        // If the chosen K is equal to the size of the dataset, return the dataset
        if (Kinput == totalPointsNumber)
            return Pinput;

        Random rand = new Random();
        int chosenPointIndex = rand.nextInt(totalPointsNumber);

        //Adding the chosen point to C
        C.add(Pinput.get(chosenPointIndex));

        //Removing the chosen point from P
        Pinput.remove(chosenPointIndex);

        //ciclo di scelta dei k centers
        for(int i = 1; i< Kinput; i++){
            int indexFarthestPoint = -1;
            double maxDistance = 0;

            //ciclo di scansione delle distanze per ogni punto in P-C
            for(int cnt = 0; cnt < Pinput.size(); cnt++){
                double minimalDistance=-1;

                //ciclo delle distanze di ogni punto in P da ogni punto di C
                for(int m = 0; m<C.size(); m++){
                    double distSElement = Vectors.sqdist(Pinput.get(cnt), C.get(m));
                    //selecting the minimal distance, in the pool of the distances between point and already chosing centers
                    if(minimalDistance < 0 || distSElement < minimalDistance ){
                        minimalDistance = distSElement;
                    }
                }

                //choosing the point that is farthest from centers set
                if(maxDistance < minimalDistance){
                    maxDistance = minimalDistance;
                    indexFarthestPoint=cnt;
                }
            }

            C.add(Pinput.get(indexFarthestPoint));

            Pinput.remove(indexFarthestPoint);
         }

        System.out.println("* Chosen centers :");
        for (int tmp = 0; tmp< Kinput; tmp++){
            System.out.println("* "+ C.get(tmp));
        }
        System.out.println("* Kcenter terminated returning centers vector.");

        //return the centers set
        return C;
    }//end Kcenter




    //Method kMeansPP
    public static ArrayList<Vector> kMeansPP(ArrayList<Vector> Pinput, ArrayList<Long> WPinput,int Kinput){

//fanciness
        System.out.println("Applyng the Kmeans++ Alg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//
//diagnostica
//        System.out.println("dimensione di p prima di un po' = "+ Pinput.size());

        //Algoritmo kmeans++
        //building C, the set of chosen points
        ArrayList<Vector> C = new ArrayList<Vector>();

        //choosing a random point in the set P
        int totalPointsNumber = Pinput.size();
        Random rand = new Random();
        int chosenPointIndex = rand.nextInt(totalPointsNumber);

        //Adding the chosen point to C
        C.add(Pinput.get(chosenPointIndex));

        //Removing the chosen point from P, and the relative weigth from WP
        Pinput.remove(chosenPointIndex);
        WPinput.remove(chosenPointIndex);


//dioagnostica
//        System.out.println("dimensione P = "+ Pinput.size()+" dimensione WP = "+ WPinput.size());
//

        //for each center chosable
        for(int i = 1; i< Kinput; i++){
            //variables used and explained later
            double minDP = -1;
            ArrayList<Double> DPvector = new ArrayList<Double>();
            double sumWeightedMinimalDistancesSquared = 0;
            ArrayList<Double> probabilityVector = new ArrayList<Double>();

            //for each ponit in P-C
            for(int cnt = 0; cnt < Pinput.size(); cnt++){

                //ciclo delle distanze di ogni punto in P da ogni punto di C
                for(int m = 0; m<C.size(); m++) {

                    //choose the min distance of ponit p, from each point in C
                    double tempDP = Vectors.sqdist(Pinput.get(cnt), C.get(m));
                    if (minDP < 0 || minDP < tempDP) {
                        minDP = tempDP;
                    }
                }

                //put each minimal distance in a vector
                DPvector.add(minDP);
            }//end of foreach point in P
//diagnosrtica
//            System.out.println("dimensione DPVector = "+ DPvector.size());
//
            //now i have all the distances and i can evaluate the probabilities
            //sum of all Squared distances weigthed
            int t=0;
            for (double dist : DPvector) {
                sumWeightedMinimalDistancesSquared += WPinput.get(t)*dist*dist;
                t++;
            }

            //calculate for each ponit in P the probability to be choosen
            double tempSum = 0;
            for(int k =0; k< DPvector.size();k++){
                double probP = (WPinput.get(k)*DPvector.get(k)*DPvector.get(k))/sumWeightedMinimalDistancesSquared;


                //adding to the probability vector each prob, adding each new prob to the sum of all the previous. that way the last element of the vector should be the value 1.
                //NB: it should be 1, but for rounding problems it does not. But the error is < 10^-12, so i cansider it trascurable
                tempSum+=probP;
                probabilityVector.add(tempSum);
            }

//diagnostica
//            System.out.println("vettore delle distanze minime = "+DPvector.size()+" vettore delle probabilità = " +DPvector.size()+" somma delle probabilità = " + probabilityVector.get(probabilityVector.size()-1));
//            System.out.println("dimensione di p prima di essere mangiato da kmeans++ = "+ Pinput.size());
//
            //now choose a pont using its probability(take a random (0,1), then look for the point which probability range in probability vector contais it)
            Random rand1 = new Random();
            double probabilityTaken = rand1.nextDouble();
            int indexOfElectedPoint = -1;
            for(int m =0; m < Pinput.size(); m++){
                if(probabilityTaken < probabilityVector.get(m)){
//diagnostica
//                   System.out.println("arrivato a prob = "+ probabilityVector.get(m));
//
                    indexOfElectedPoint = m;

                    //selecting the center, removing it from P and from Weigth vector
                    C.add(Pinput.get(indexOfElectedPoint));
                    Pinput.remove(indexOfElectedPoint);
                    WPinput.remove(indexOfElectedPoint);
                    break;
                }
            }

//diagnostica
//            System.out.println("Punto eletto = " + indexOfElectedPoint+" con probabilità ="+ probabilityTaken+ " e dimensione del set p =" + Pinput.size());

//diagnosrtica
 //           System.out.println("probabilità pescata = "+ probabilityTaken);
  //          System.out.println("punto scelto alla fine = "+ indexOfElectedPoint);
//


        }//end of center selection cicle



        //dioagnostica
        System.out.println("Printing the  "+ Kinput +" centers : ");
        for (int tmp = 0; tmp< Kinput; tmp++){
            System.out.println(" "+ C.get(tmp));
        }
        System.out.println("Kmeans++ terminated returning  centers vector.");

        //return the centers set
        return C;

    }//end of kcenter


    //Method kMeansObj
    public static double kMeansObj(ArrayList<Vector> Pinput,ArrayList<Vector> Cinput){

//fanciness
        System.out.println("Applyng the KmeansOBJ Alg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//
        double dist;
        double minDist=-1;
        double totMinDistances = 0;
        //for each point that is not a center
        for (Vector p: Pinput) {
            //for each center
            for (Vector c: Cinput) {
                dist = Vectors.sqdist(p,c);
                if(minDist < 0 || dist < minDist){
                    minDist = dist;
                }
            }
            totMinDistances += minDist;
        }

        // average of minimal distances on the total number of non-center points
        double avgDist = totMinDistances/Pinput.size();
        return avgDist;
    }//end of kmeansobj



    //metodo di selezione del file
    private static ArrayList<Vector> choosingFile(int sChosenFileMethod) throws IOException{
        switch (sChosenFileMethod) {
            case 1:
                return InputOutput.readVectorsSeq("test.txt");
            case 2:
                return InputOutput.readVectorsSeq("vecs-50-50000.txt");
            case 3:
                return InputOutput.readVectorsSeq("vecs-50-100000.txt");
            case 4:
                return InputOutput.readVectorsSeq("vecs-50-500000.txt");

            default:
                System.out.println("Invalid algorithm number. Using the smaller set");
                return InputOutput.readVectorsSeq("vecs-50-10000.txt");
        }
    }
}


//mettere il file in P

