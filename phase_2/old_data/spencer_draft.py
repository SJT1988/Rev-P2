# 1. Relocate our project directory to your WSL user directory.
# 2. Open the project folder (Rev-P2) in VS Code.

import pyspark
from pyspark.sql import SparkSession
import sys
import os
import re
import pandas as pd
import numpy as np

# FILEPATH VARIABLES

root_path = 'file:/home/strumunix/' # CHANGE THIS
local_dir = 'Rev-P2/phase_2/'
moviesFilename = root_path + local_dir + "movies.dat"
ratingsFilename = root_path + local_dir + "ratings.dat"
usersFilename = root_path + local_dir + "users.dat"

# SPARK VARIABLES:

spark = SparkSession.builder \
    .master("local")\
    .appName("PySpark_RDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext

#================================================================================
# PREPARING DATA

#--------------------------------------------------------------------------------
# set textFiles
rawRatings = sc.textFile(ratingsFilename)
rawMovies = sc.textFile(moviesFilename)
rawUsers = sc.textFile(usersFilename)

#--------------------------------------------------------------------------------
# functions to split a string/row/record/line-of-data into tuples of required
# data type:

def get_ratings_tuple(entry):
    items = entry.split('::')
    return int(items[0]), int(items[1]), int(items[2]), int(items[3])

def get_movie_tuple(entry):
    items = entry.split('::')
    return int(items[0]), str(items[1]), str(items[2])

def get_users_tuple(entry):
    items = entry.split('::')
    return int(items[0]), str(items[1]), int(items[2]), int(items[3]), str(items[4])

#--------------------------------------------------------------------------------
# take the datafiles (Raw-), map them using get- functtions into RDD's:
ratingsRDD = rawRatings.map(get_ratings_tuple).cache()
moviesRDD = rawMovies.map(get_movie_tuple).cache()
usersRDD = rawUsers.map(get_users_tuple).cache()

#--------------------------------------------------------------------------------
# get the record counts for each datafile (I didn't use this yet)
ratingsCount = ratingsRDD.count()
moviesCount = moviesRDD.count()
usersCount = usersRDD.count()

#--------------------------------------------------------------------------------
# verify things are as they should be so far:
print('There are %s ratings, %s movies and %s users in the datasets' % (ratingsCount, moviesCount, usersCount))
print('Ratings: %s' % ratingsRDD.take(3))
print('Movies: %s' % moviesRDD.take(3))
print('Users: %s' % usersRDD.take(3))

assert ratingsCount == 1000209
assert moviesCount == 3883
assert usersCount == 6040
#assert moviesRDD #.filter(lambda id, title: title == 'Toy Story (1995)').count() == 1
#assert (ratingsRDD.takeOrdered(1, key=lambda user, movie, rating: movie) == [(1, 1, 5.0)])


#=================================================================================
# create datafranes from the RDD's. Define schema as best I can at this stage:
movies_df = spark.createDataFrame(moviesRDD, schema = ['movie_id','movie_name','movie_genre'])
ratings_df = spark.createDataFrame(ratingsRDD, schema = ['user_id','movie_id','rating','mystery_a'])
users_df = spark.createDataFrame(usersRDD, schema = ['user_id','gender','mystery_b','mystery_c','mystery_d'])

#=================================================================================
#=================================================================================



'''
----------------------------------------------------------------------------------
Analytical Queries
----------------------------------------------------------------------------------
'''
print('\n\n\n')
'''1-1  What are the top 10 most viewed movies?'''
#---------------------------------------------------------------------------------
'The top 10 most viewed movies are (presumably the ones most rated):'
most_watched_df = ratings_df.groupBy('movie_id') \
    .count() \
    .sort('count',ascending=False) \
    .limit(10) \
    .join(movies_df, ratings_df.movie_id == movies_df.movie_id, 'inner') \
    .select('movie_name','count') \
    .show(truncate=False)
print('\n\n\n')

del most_watched_df



'''1-2  What are the distinct list of genres available?'''
#---------------------------------------------------------------------------------
genre_list = []
temp_list = list(movies_df.select('movie_genre').toPandas()['movie_genre'])
for val in temp_list:
    lst = val.split('|')
    for s in lst:
        if s not in genre_list:
            genre_list.append(s)
del temp_list
print('the distinct movie genres are:')
m = 4
paragraph = [genre_list[i:i+m] for i in range(0, len(genre_list), m)]
for line in paragraph:
    for genre in line:
        print(genre, end=', ')
    print()
print('\n\n')

del m
del paragraph



'''1-3  How many movies for each genre?'''
#---------------------------------------------------------------------------------
genre_count = {
    'Animation': 0, 'Children\'s': 0, 'Comedy': 0, 'Adventure': 0,
    'Fantasy': 0, 'Romance': 0, 'Drama': 0, 'Action': 0,
    'Crime': 0, 'Thriller': 0, 'Horror': 0, 'Sci-Fi': 0,
    'Documentary': 0, 'War': 0, 'Musical': 0, 'Mystery': 0,
    'Film-Noir': 0, 'Western': 0
}
nested_genre_list = list(movies_df.select('movie_genre').toPandas()['movie_genre'])
for val in nested_genre_list:
    lst = val.split('|')
    for s in lst:
        genre_count[s]+=1
del nested_genre_list

sorted_count = dict(sorted(genre_count.items(), key=lambda val: val[1], reverse = True))
del genre_count

print('Movie totals for each genre:')
for entry in sorted_count:
    print(entry + ': ' + str(sorted_count[entry]))
print('\n\n\n')
del sorted_count

'''
---------------------------------------------------------------------------------
Analytical Queries
---------------------------------------------------------------------------------
1-4 How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
1-5 List the latest released movies
---------------------------------------------------------------------------------
'''

spark.stop()