
val yt_raw_data = sc.textFile("D:\\CSE(AI)\\SEM-4\\BIG DATA ANALYTICS\\Project\\channels_upd.csv")
val header = yt_raw_data.first()
val df = header.split(',')
df.length
val yt_data = yt_raw_data.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
val yt_rdd = yt_data.map{l =>
     val s = l.split(',')
     val (category_id, category_name, channel_id, followers, join_date, picture_url, profile_url, trailer_url, videos)=(s(0).toInt, s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8).toInt)
     (category_id, category_name, channel_id, followers, join_date, picture_url, profile_url, trailer_url, videos)}
yt_rdd.toDF.show	 
	 
//Analysis of whole dataset

val join_date_asc_rdd = yt_rdd.sortBy(_._5)
println("The channel creation history in ascending order")
join_date_asc_rdd.toDF.show(10)


val most_videos_rdd = yt_rdd.sortBy(_._9, false)
println("The top 10 channels which uploaded most no. of videos")
most_videos_rdd.toDF.show(10)


val least_videos_rdd = yt_rdd.sortBy(_._9)
println("The 10 channels that uploaded the least no. of videos")
least_videos_rdd.toDF.show(10)

val most_followers_rdd = yt_rdd.sortBy(_._4, false)
println("The top 10 channels with most fan following")
most_followers_rdd.toDF.show(10)


// Analysis of channels belonging to a particular category_name

val name = "Sports"
val sports_rdd = yt_rdd.filter(l => l._2.contains(name))
val count_sports = sports_rdd.count()
println("The total no. of sports channels in youtube are: " + count_sports)

val most_sports_rdd = sports_rdd.sortBy(_._9, false)
println("The 10 channels under sports category uploading the most videos are")
most_sports_rdd.toDF.show(10)

val sports_followers = sports_rdd.sortBy(_._4,false)
println("The top 10 sports channels with max. followers are")
sports_followers.toDF.show(10)

// Analysis of channels based on join date


val spec_date = yt_rdd.filter(l => l._5.contains("17-09-2008"))
spec_date.toDF.show
println("No. of channels which joined on Semptember 17th, 2008 : " + spec_date.count())