package net.talqum.hadoop.mahout.recommender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class Main {

    public static void main(String[] args) {
	// write your code here

        Main prog = new Main();
        prog.start();
    }

    public void start(){
       // convertDataToCSV("u.data", "data.csv");

 //      System.out.println("Item:");
 //       ItemRecommender itemRecommend = new ItemRecommender();
//        itemRecommend.recommend();

        System.out.println("User:");
        UserRecommender userRecommender = new UserRecommender();
        userRecommender.recommend();
    }

    private void convertDataToCSV(String infile, String outfile){
        try(BufferedReader br =  new BufferedReader(new FileReader(infile));
            BufferedWriter bw = new BufferedWriter(new FileWriter(outfile))){

            String line;
            while((line = br.readLine()) != null){
                String[]values = line.split("\\t", -1);
                bw.write(values[0] + "," + values[1] + "," + values[2] + System.lineSeparator());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
