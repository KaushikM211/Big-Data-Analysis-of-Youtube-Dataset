val yt_raw_data = sc.textFile("D:\\CSE(AI)\\SEM-4\\BIG DATA ANALYTICS\\Project\\INvideos_upd.csv")
val header = yt_raw_data.first()
val df = header.split(',')
df.length
val yt_data = yt_raw_data.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
val yt_rdd = yt_data.map{l =>
     val s = l.split(',')
     val (video_id, trending_date, category_id, publish_time, views, likes, dislikes, comment_count)=(s(0), s(1), s(2), s(3), s(4).toInt, s(5).toInt, s(6).toInt, s(7).toInt)
     (video_id, trending_date, category_id, publish_time, views, likes, dislikes, comment_count)}
yt_rdd.toDF.show	

//Analysis of whole dataset

val most_liked = yt_rdd.sortBy(_._6,false)
print("Top 10 most liked video on youtube in India")
most_liked.toDF.show(10)


val most_views = yt_rdd.sortBy(_._5,false)
print("Top 10 most viewed video on youtube in India")
most_views.toDF.show(10)


val least_liked = yt_rdd.sortBy(_._7,false)
print("Top 10 most disliked video on youtube in India")
least_liked.toDF.show(10)


val most_commented = yt_rdd.sortBy(_._8,false)
print("Top 10 most commented video on youtube in India")
most_commented.toDF.show(10)



// Analysis corresponding to a specific date


val spec_date = yt_rdd.filter(l => l._2.contains("18.18.02"))
print("The videos trending on youtube on Feb 18,2018")
spec_date.toDF.show


val count_spec_date = spec_date.count()
println("The total videos recorded in the dataset on Feb 18, 2018 is: " + count_spec_date)


val most_liked_spec_date = spec_date.sortBy(_._6,false)
print("Top 10 most liked video on youtube in India")
most_liked_spec_date.toDF.show(10)


val most_views_spec_date = spec_date.sortBy(_._5,false)
print("Top 10 most viewed video on youtube in India on Feb 18, 2018 ")
most_views_spec_date.toDF.show(10)


val least_liked_spec_date = spec_date.sortBy(_._7,false)
print("Top 10 most disliked video on youtube in India on Feb 18, 2018 ")
least_liked_spec_date.toDF.show(10)


val most_commented_spec_date = spec_date.sortBy(_._8,false)
print("Top 10 most commented video on youtube in India on Feb 18, 2018 ")
most_commented_spec_date.toDF.show(10)


//Adding Additional parameter(Ratio of likes to views and that of dislikes to views) and storing it into a new RDD

val yt_rdd_modified = yt_rdd.map{l=>
	val (video_id, trending_date, category_id, publish_time, views, likes, dislikes, comment_count , likes_to_views, dislikes_to_views) = (l._1,l._2,l._3,l._4,l._5,l._6,l._7,l._8,l._6/l._5.toDouble,l._7/l._5.toDouble)
	(video_id, trending_date, category_id, publish_time, views, likes, dislikes, comment_count, likes_to_views, dislikes_to_views)
}
yt_rdd_modified.toDF.show(10)

val yt_best_videos = yt_rdd_modified.sortBy(_._9,false)
println("The top 10 videos of the trending videos dataset")
yt_best_videos.toDF.show(10)



