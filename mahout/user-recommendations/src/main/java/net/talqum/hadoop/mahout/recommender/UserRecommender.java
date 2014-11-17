package net.talqum.hadoop.mahout.recommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * User similarity based recommendations
 * Created by Imre on 2014.11.08..
 */
public class UserRecommender {

    /**
     * Data format:
     * userID, itemID, value(e.g. the rating given to a movie)
     *
     * The idea behind this approach is that when we want to compute recommendations for a particular users,
     * we look for other users with a similar taste and pick the recommendations from their items.
     */

    public void recommend() {
        try {
            DataModel dataModel = new FileDataModel(new File("data.csv"));

            /**
             * For finding similar users, we have to compare their interactions. There are several methods for doing this.
             * One popular method is to compute the correlation coefficient between their interactions.
             */
            UserSimilarity sim = new PearsonCorrelationSimilarity(dataModel);

            /**
             * The next thing we have to do is to define which similar users we want to leverage for the recommender.
             * For the sake of simplicity, we'll use all that have a similarity greater than 0.1.
             * This is implemented via a ThresholdUserNeighborhood:
             */
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, sim, dataModel);

            UserBasedRecommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, sim);

            /**
             * We can easily ask the recommender for recommendations now.
             * If we wanted to get 15 items recommended for the user with userID 2, we would do it like this:
             */
            List<RecommendedItem> recommendations = recommender.recommend(2, 15);

            for (RecommendedItem recommendation : recommendations) {
                System.out.println(recommendation);
            }
        } catch (TasteException | IOException e1) {
            e1.printStackTrace();
        }
    }
}
