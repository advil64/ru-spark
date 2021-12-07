### RU-Spark

#### By Advith Chegu (ac1771)

**Q:** For each program, describe what transformations you did to get to the result.

**A:** Let's go through each program and describe them one by one

*Reddit Photo Impact ->* For this program I had to find the total interaction with each photo across different subreddits. I started by loading any given csv file into a spark dataset (dataframe). I used the infer schema option so that the numerical values would keep their type when being read into the dataframe.

Next I used the `groupby` function on the image id column of the dataframe to aggregate all of the interaction indicators (likes, dislikes, and comments) for each image id. Lastly, I summed the three aggregated columns and renamed the new column appropriately and printed the resulting dataframe to output.

