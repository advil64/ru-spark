### RU-Spark

#### By Advith Chegu (ac1771)

**Q:** For each program, describe what transformations you did to get to the result.

**A:** Let's go through each program and describe them one by one

*Reddit Photo Impact ->* For this program I had to find the total interaction with each photo across different subreddits. I started by loading any given csv file into a spark dataset (dataframe). I used the infer schema option so that the numerical values would keep their type when being read into the dataframe.

Next I used the `groupby` function on the image id column of the dataframe to aggregate all of the interaction indicators (likes, dislikes, and comments) for each image id. Lastly, I summed the three aggregated columns and renamed the new column appropriately and printed the resulting dataframe to output.

*Reddit Hour Impact ->* For this program I had to now figure out the aggregate of the likes, dislikes, and comments but for each hour in the day. Here I decided to chose a different route for datasets and decided to use sql instead of what was used previously. I started using the same steps and loaded the given csv into a spark dataset object.

Next however, I used the `spark.sql` function and used a build in sql function called `HOUR` which basically extracted the hour of the day from the timestamp provide, from there I was able to `GROUP` in sql and get the aggregates of the interactions by using the `SUM` function for the chosen columns.

*Netflix Movie Average ->* 

*Netflix Graph Generate ->*

**Q:** Based on the result from RedditPhotoImpact, what was the most impactful photo for the whole data set?

**A:** The image with the greatest photo impact was image number 1437 with a total impact of 192896.

**Q:** Based on the result from RedditHourImpact, what hours of the day (in EST) did submitted
posts have the most impact/reach?

**A:** Hour 20 seems to be the busiest hour on reddit with a total hour impact of 15057971 which is the amount of likes, comments, and dislikes combined. This hour converted to 12hr time would be between 8-9 PM.

**Q:** Based on the result from NetflixMovieAverage, name 2-3 movies that had the highest average rating.

**A:** The following movies were at the top of the list when it came to their average rating:

|movie_id|average_rating|
|--------|--------------|
|14961|4.72|
|7230|4.72|
|7057|4.70|
|3456|4.67|
|9864|4.64|