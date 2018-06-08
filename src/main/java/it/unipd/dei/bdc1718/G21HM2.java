

/*
* We Implemented WordCountImproved1, WordCountImproved2, and we adapted WordCountImproved2 to use reduceByKey method.
* Clearly we didn't expect to get better execution time with WordCountImproved2. Moreover as the results show it is slower.
* What we were aiming for was to reduce the main memory usage, penalising running time by doing more data structure iterative accesses.
* Anyway we tried to reduce the running time by inserting the most common words in the first place of the arrays. In this way all
* the successive accesses, in order to check already gathered words, were more probably to find a match in fewer iterations.
*/


package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;


public class G21HM2 {



    public static void main(String[] args){

        //SPARK CONFIGURATION
        SparkConf configuration = new SparkConf(true)
                .setAppName("WordCount")
                .setMaster("local[*]");

        //SPARK CONTEXT
        JavaSparkContext sc = new JavaSparkContext(configuration);

        //ENCODE IN A RDD THE FILE. Moreover setting up to mesure time
        JavaRDD<String> lines = sc.textFile("text-sample.txt").cache();
        lines.count();

        //Algorithm choice
        String sChooseAlgo = "Choose a word count algorithm\n" +
                "1 - Improved Word Count 1.\t" +
                "2 - Improved word count 2.\t" +
                "3 - Reduce by key word count.\t" +
                "9 - Exit.";

        //Scanner and variables
        Scanner consoleInput = new Scanner(System.in);
        int iNumberOfWordsToShow = 0;
        long start = 0;
        long end = 0;
        JavaPairRDD<String, Long> wordCount;

        // Total number of words in the text file
        long lTotalWords = 0L;
        // For each row of the JavaRDD count the strings and update the total word counter
        for (String row : lines.collect())
        {
            String [] stringArray = row.split(" ");
            lTotalWords += stringArray.length;
        }
        //Compute the squared root of the total number of words, to do wordcount2
        final long squaredRootN = (long)Math.sqrt(lTotalWords);


        while (true) {
            System.out.println(sChooseAlgo);
            int iChosenAlgo = consoleInput.nextInt();
            System.out.println("Chosen algorithm number " + iChosenAlgo + ". Ongoing elaboration of input file...");
            switch (iChosenAlgo) {

                //WordCountImproved1
                case 1:
                    //Check time
                    start = System.currentTimeMillis();
                    wordCount = ImprovedWordCount1(lines,squaredRootN);
                    end = System.currentTimeMillis();
                    System.out.println("Elapsed time " + (end - start) + " ms");

                    //Asking the user a number of "most common words"
                    System.out.println("Enter the number of the most frequent words you want to inspect:");
                    iNumberOfWordsToShow = consoleInput.nextInt();
                    GetFrequentWords(wordCount, iNumberOfWordsToShow);
                    break;

                //WordCountImproved2
                case 2:
                    start = System.currentTimeMillis();
                    wordCount = ImprovedWordCount2(lines,squaredRootN);
                    end = System.currentTimeMillis();
                    System.out.println("Elapsed time " + (end - start) + " ms");
                    System.out.println("Enter the number of the most frequent words you want to inspect:");
                    iNumberOfWordsToShow = consoleInput.nextInt();
                    GetFrequentWords(wordCount, iNumberOfWordsToShow);
                    break;

                //WordCountImproved2ReduceByKey
                case 3:
                    start = System.currentTimeMillis();
                    wordCount = ReduceByKeyWordCount(lines,squaredRootN);
                    end = System.currentTimeMillis();
                    System.out.println("Elapsed time " + (end - start) + " ms");
                    System.out.println("Enter the number of the most frequent words you want to inspect:");
                    iNumberOfWordsToShow = consoleInput.nextInt();
                    GetFrequentWords(wordCount, iNumberOfWordsToShow);
                    break;

                case 9:
                    System.out.println("Exiting...");
                    return;

                default:
                    System.out.println("Invalid algorithm number.");
                    break;


            }//endswitch
            System.out.println("*********************************************************************************************************");
        }//endloop
    }//endmain











    public static JavaPairRDD ImprovedWordCount1(JavaRDD<String> lines,long squaredRootN) {
        JavaPairRDD<String, Long> wordcounts = lines

                //Map Phase: Create an Arraylist composed by tuple2 (word,occurrency), and return an iterator to it
                .flatMapToPair((document) -> {

                    //Create an array of words of the documents
                    String[] tokens = document.split(" ");

                    //create an arraylist of (word occurrency)
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    //The cycle add words to the arraylist with the relative sum of all occurrency, one tuple for each different word
                    for (String token : tokens) {

                        //check if araylist is empty. ift it is add the first word
                        if(pairs.size()==0){ pairs.add(new Tuple2<>(token, 1L));continue;}

                        //check if the word is already in arraylist
                        Boolean bTupleAlreadyPresent = false;
                        for(int i = 0; i<pairs.size();i++){

                            //The word is already contained
                            if(pairs.get(i)._1().equals(token)){
                                Long cnt = pairs.get(i)._2()+1;
                                pairs.remove(i);

                                //NB:add the word at the beginning of the araylist. So the most common words are the first accessed in the next scan
                                pairs.add(0,new Tuple2<>(token, cnt));
                                bTupleAlreadyPresent=true;
                                break;
                            }
                        }
                        //normal adding
                        if(!bTupleAlreadyPresent){
                            pairs.add(new Tuple2<>(token, 1L));
                        }
                    }

                    //return an iterator to the Arraylist
                    return pairs.iterator();
                })

                //Reduce phase
                .groupByKey()                       //Group up by words, and build array of occurrency for each word.
                .mapValues((it) -> {                //sum up the occurrency array for each word
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        //Computing number of different words
        System.out.println("There are " + wordcounts.count() + " different words in the input file");
        return wordcounts;
    }






    public static JavaPairRDD ImprovedWordCount2(JavaRDD<String> lines, long squaredRootN) {

        JavaPairRDD<String, Long> wordcounts = lines

                //Round 1 MapPhase, start as wordcount1, but then build an improved arraylist
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        if(pairs.size()==0){ pairs.add(new Tuple2<>(token, 1L));}
                        Boolean bTupleAlreadyPresent = false;
                        for(int i = 0; i<pairs.size();i++){
                            if(pairs.get(i)._1().equals(token)){
                                Long cnt = pairs.get(i)._2()+1;
                                pairs.remove(i);
                                pairs.add(0,new Tuple2<>(token, cnt));
                                bTupleAlreadyPresent=true;
                                break;
                            }
                        }
                        //normal adding
                        if(!bTupleAlreadyPresent){
                            pairs.add(new Tuple2<>(token, 1L));
                        }
                    }

                    //This code build an arraylist(random,(word,occurrencies)). where rand is a random value picked in [0,sqrtN), where N is the number of total word
                    ArrayList<Tuple2<Long, Tuple2<String,Long>>> improvedPairs = new ArrayList<>();
                    for (Tuple2<String,Long> pair : pairs){
                        Long rand = Long.valueOf((long)(Math.random()*(squaredRootN)));
                        improvedPairs.add(new Tuple2<Long,Tuple2<String, Long>>(rand,pair));
                    }

                    //Return an iterator to the upgraded arraylist
                    return improvedPairs.iterator();
                })
                //Round 1 ReducePhase
                .groupByKey()                       // Group up by randomKey the tuples (word,occurrencies)
                .flatMapToPair((it) -> {            //invert the Tuples producing  (word, ocurrency) where occurency is the sum up of each occuerrency for each Key
                    ArrayList<Tuple2<String,Long>> reversedImprovedPairs = new ArrayList<>();

                    //Scan all the array of tuples for each key
                    for (Tuple2 t : it._2()) {

                        //take word and occurrency
                        String w = ((String) t._1());
                        Long l = (long)t._2();

                        //check if araylist is empty. ift it is add the first word
                        if(reversedImprovedPairs.size()==0){
                            reversedImprovedPairs.add(new Tuple2<>(w, l));
                            continue;
                        }

                        //check if word is already added.
                        Boolean bTupleAlreadyPresent = false;
                        for(int i = 0; i<reversedImprovedPairs.size();i++) {
                            //Already contained
                            if (reversedImprovedPairs.get(i)._1().equals(w)){
                                Long cnt = reversedImprovedPairs.get(i)._2() + l;
                                reversedImprovedPairs.remove(i);

                                //NB:add the tuples at the beginning of the araylist. So the most common tuples are the first accessed in the next scan
                                reversedImprovedPairs.add(0,new Tuple2<>(w, cnt));
                                bTupleAlreadyPresent = true;
                                break;
                            }
                        }
                        //New tuple
                        if(!bTupleAlreadyPresent){
                            reversedImprovedPairs.add(new Tuple2<>(w, l));
                        }
                    }

                    //return the reversed arraylist
                    return reversedImprovedPairs.iterator();
                })
                //Round2.No MapPhase. Group by key using word as key
                .groupByKey()
                .mapValues((it) -> { //sum up the occurrency array
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        System.out.println("There are " + wordcounts.count() + " different words in the input file");
        return wordcounts;
    }





    public static JavaPairRDD ReduceByKeyWordCount(JavaRDD<String> lines, long squaredRootN) {
        JavaPairRDD<String, Long> wordcounts = lines

                //Round 1 MapPhase, do the same as wordcount2 MapPhase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        if(pairs.size()==0){ pairs.add(new Tuple2<>(token, 1L));}
                        Boolean bTupleAlreadyPresent = false;
                        for(int i = 0; i<pairs.size();i++){
                            if(pairs.get(i)._1().equals(token)){
                                Long cnt = pairs.get(i)._2()+1;
                                pairs.remove(i);
                                pairs.add(0,new Tuple2<>(token, cnt));
                                bTupleAlreadyPresent=true;
                                break;
                            }
                        }
                        if(!bTupleAlreadyPresent){
                            pairs.add(new Tuple2<>(token, 1L));
                        }
                    }

                    ArrayList<Tuple2<Long, Tuple2<String,Long>>> improvedPairs = new ArrayList<>();
                    for (Tuple2<String,Long> pair : pairs){
                        Long rand = Long.valueOf((long)(Math.random()*(squaredRootN)));
                        improvedPairs.add(new Tuple2<Long,Tuple2<String, Long>>(rand,pair));
                    }

                    return improvedPairs.iterator();
                })
                .groupByKey()                       // Group up the tuples (word,occurrencies) for each Key random
                .flatMapToPair((it) -> {            //revert the Tuples producing  (word, ocurrency) where occurency is the sum up of each occuerrency for each Key
                    ArrayList<Tuple2<String,Long>> reversedImprovedPairs = new ArrayList<>();

                     for (Tuple2 t : it._2()) {

                        String w = ((String) t._1());
                        Long l = (long)t._2();

                        if(reversedImprovedPairs.size()==0){
                            reversedImprovedPairs.add(new Tuple2<>(w, l));
                            continue;
                        }

                        Boolean bTupleAlreadyPresent = false;
                        for(int i = 0; i<reversedImprovedPairs.size();i++) {
                            if (reversedImprovedPairs.get(i)._1().equals(w)){
                                Long cnt = reversedImprovedPairs.get(i)._2() + l;
                                reversedImprovedPairs.remove(i);
                                reversedImprovedPairs.add(0,new Tuple2<>(w, cnt));
                                bTupleAlreadyPresent = true;
                                break;
                            }
                        }
                        if(!bTupleAlreadyPresent){
                            reversedImprovedPairs.add(new Tuple2<>(w, l));
                        }
                    }

                    return reversedImprovedPairs.iterator();
                })
                .reduceByKey((x,y) -> x + y);       // <-- Reduce phase using reduceByKey

        System.out.println("There are " + wordcounts.count() + " different words in the input file");
        return wordcounts;
    }










    //Managing the most common word request using the metod .top(). It uses the comparator class defined at the end
    public static void GetFrequentWords(JavaPairRDD<String, Long> wordCount, int iNumberOfWords)
    {
        List<Tuple2<String,Long>> sortedtTupleList = wordCount.top(iNumberOfWords, new Tuple2Comparator());
        System.out.println("The " + iNumberOfWords + " more frequent word are:");
        for (Tuple2 tuple : sortedtTupleList)
            System.out.println("Word: \"" + tuple._1() + "\" number of occurrences: " + tuple._2());
    }










    //Comparator class used to find the words with max number of occurrence
    public static class Tuple2Comparator implements Serializable, Comparator<Tuple2<String,Long>> {

        public int compare(Tuple2<String,Long> firstTuple, Tuple2<String,Long> secondTuple)
        {
            if (firstTuple._2() > secondTuple._2())
                return 1;
            else if (firstTuple._2() < secondTuple._2())
                return -1;
            return 0;
        }
    }
}

