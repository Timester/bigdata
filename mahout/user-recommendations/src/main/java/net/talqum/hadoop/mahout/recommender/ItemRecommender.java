package net.talqum.hadoop.mahout.recommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Item similarity based recommender
 * Created by Imre on 2014.11.08..
 */
public class ItemRecommender {

    /**
     * Data format:
     * userID, itemID, value(e.g. the rating given to a movie)
     */
    public void recommend(){
        try {
            DataModel dataModel = new FileDataModel(new File("data.csv"));

            ItemSimilarity sim = new LogLikelihoodSimilarity(dataModel);
            // TanimotoCoefficientSimilarity sim = new TanimotoCoefficientSimilarity(dataModel);

            ItemBasedRecommender recommender = new GenericItemBasedRecommender(dataModel, sim);

            int x = 1;
            for(LongPrimitiveIterator items = dataModel.getItemIDs(); items.hasNext();){
                long itemId = items.nextLong();
                List<RecommendedItem> recommendations = recommender.mostSimilarItems(itemId, 5);

                for(RecommendedItem recommendation : recommendations){
                    System.out.println(itemId + "," + recommendation.getItemID() + "," + recommendation.getValue());
                }
                x++;
                if(x >10){
                    System.exit(0);
                }
            }
        } catch (IOException | TasteException e) {
            e.printStackTrace();
        }
    }

}
