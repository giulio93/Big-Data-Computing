package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

import static java.lang.Math.abs;

public class G21HM1 {
    public static void main(String[] args) throws  FileNotFoundException {
        if(args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on command line");
        }

        //Read a list of numbers from the program options
        ArrayList<Double> lNumbers = new ArrayList<>();
        Scanner s = new Scanner(new File(args[0]));
        while(s.hasNext()){
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        //Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);





        /* ************************************************************************************************************
         * Group code:
         * (1)Already managed by the code written above^
         */


        //(2) Create a new JavaRDD dDiffavgs containing the absolute value of the difference between each element of
        // dNumbers and the arithmetic mean of all values in dNumbers.

        // Calculate N the size of the set
        long size = dNumbers.count();

        // Calculate the average using one round MapReduce
        double avg = dNumbers.map(x -> x/size).reduce((x,y) -> x + y);

        // Generate the RDD dDiffavgs containing the absolute value of the difference between each element of dNumbers
        // and the arithmetic mean of all values in dNumbers
        JavaRDD<Double> dDiffavgs = dNumbers.map(x -> abs(x-avg));

        //(3) Compute and print the minimum value in dDiffavgs. Do it in two ways:
        //using the reduce method;
        //using the min method of the JavaRDD class passing to it a comparator as explained here.


        // Compute and print the min value in dDiffavgs using the reduce method
        double mindDiffavgs = dDiffavgs.reduce((x , y) -> x < y ? x : y);
        System.out.println("The result for min of dDiffavgs, using reduce function is: " + mindDiffavgs);


        // Compute and print the min value in dDiffavgs using min() method. Class DoubleComparator is defined at the end
        double mindDiffavgs1 = dDiffavgs.min(new DoubleComparator());
        System.out.println("The result for min of dDiffavgs, using min function is: " + mindDiffavgs1);


        //(4) Compute and print another statistics of your choice on the data in dNumbers. Make sure that you use at least
        // one more method chosen among those of the JavaRDD interface.

        // Filter and print all values of a collection which have absolute difference from the mean value
        // minor than the standard deviation

        // Compute and print standard deviation calling a ComputeStandardDeviation() defined at the end
        double dStdDev = ComputeStandardDeviation(dNumbers);
        System.out.println("The standard deviation of the collection of data is: " + dStdDev);

        //Compute and print all values of the collection which have absolute difference from the mean value
        // minor than the standard deviation
        JavaRDD<Double> dFilteredData = dNumbers.filter((x) -> (Math.abs(x - avg) < dStdDev));
        StringBuffer bestElements = new StringBuffer("");
        for (int i = 0; i < dFilteredData.count(); i++)
            bestElements.append(Double.toString(dFilteredData.collect().get(i)) + " ");

        System.out.println("The filtered data with absolute difference from the mean value minor than the standard deviation are: "+bestElements);
    }


    // Class used in the min method of dDiffavgs
    public static class DoubleComparator implements Serializable, Comparator<Double> {

        public int compare(Double a, Double b) {
            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;
        }
    }

    // Method that compute standard deviation
    public static double ComputeStandardDeviation(JavaRDD<Double> adData){
        if (adData.isEmpty())
            throw new IllegalArgumentException("Input array has no data");

        //compute the mean as before with map/reduce
        long size = adData.count();
        double avg = adData.map(x -> x/size).reduce((x,y) -> x + y);

        //compute standard deviation
        double dSquareDiffSum = 0;
        long cnt = adData.count();
        for (int i = 0; i < cnt; i++)
            dSquareDiffSum += Math.pow(adData.collect().get(i) - avg, 2);

        return Math.sqrt(dSquareDiffSum / adData.count());
    }

}












