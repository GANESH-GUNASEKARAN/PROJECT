#!/usr/bin/env python
# coding: utf-8

api_key="Enter your api_key"

from googleapiclient.discovery import build
import psycopg2
import pymongo
import pandas as pd
import streamlit as st

api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name,api_version,developerKey=api_key)


Wisdom_Frames = "UC1cJ7YO6_rBYqLtL96-_8qA"
Sp_Star_Entertainment = "UCB1fTR2KHaSWtX7Ubv3EusA"
Focus_Academy_Lectures = "UCCVl4_BNQX6EqDf1Eufu4Lw"
Sathya_Jyothi_Films = "UCdbalkQDqCcOsYG5c4L8TAw"
Vetrii_IAS_Study_Circle = "UCov_xlhFxC790TvjHm_K2jg"
Aravind_SA = "UCrJNwpevlqZLVO1LW2Mo-Ag"
Flyingmeenaboi = "UC81jnpmqzFVPDZhJGHsNTVQ"
All_About_Mechanical_Engineering = "UCaI6gazNIAsclpelpAuCynw"
HiphopTamizha = "UC3Izrk2fUSIEwdcH0kNdzeQ"
Top_Media = "UCfTUswmQObxF425UD48Paig"


def recieve_channel_stat(youtube,channel_id):
    
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
  )
  response=request.execute()
  for item in response['items']:
    data={'channelName':item['snippet']['title'],
          'channelId':item['id'],
          'subscriptionCount':item['statistics']['subscriberCount'],
          'channelViews':item['statistics']['viewCount'],
          'totalVideos':item['statistics']['videoCount'],
          'channelDescription':item['snippet']['description'],
          'playlistId':item['contentDetails']['relatedPlaylists']['uploads']
    }
  return data


def recieve_playlists(youtube,channel_id):
  request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25
    )
  response = request.execute()
  allData=[]
  for item in response['items']: #these are the details we are getting about the playlist
     data={'PlaylistId':item['id'],
           'Title':item['snippet']['title'],
           'ChannelId':item['snippet']['channelId'],
           'ChannelName':item['snippet']['channelTitle'],
           'PublishedAt':item['snippet']['publishedAt'],
           'VideoCount':item['contentDetails']['itemCount']
           }
     allData.append(data)

     next_page_token = response.get('nextPageToken')

     while next_page_token is not None:
        request = youtube.playlists().list(
              part="snippet,contentDetails",
              channelId="UCmXkiw-1x9ZhNOPz0X73tTA",
              maxResults=25)
        response = request.execute()

        for item in response['items']: #these are the details we are getting about the playlist
            data={'PlaylistId':item['id'],
                  'Title':item['snippet']['title'],
                  'ChannelId':item['snippet']['channelId'],
                  'ChannelName':item['snippet']['channelTitle'],
                  'PublishedAt':item['snippet']['publishedAt'],
                  'VideoCount':item['contentDetails']['itemCount']}
            allData.append(data)
        next_page_token = response.get('nextPageToken')
  return allData


def get_video_details(youtube, videoIds):

   
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=videoIds
    )
    response = request.execute()

    for video in response['items']:
        stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt','channelId'],
                          'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                          'contentDetails': ['duration', 'definition', 'caption']
                        }
        video_info = {}
        video_info['video_id'] = video['id']

        for k in stats_to_keep.keys():
            for v in stats_to_keep[k]:
                try:
                    video_info[v] = video[k][v]
                except:
                    video_info[v] = None

            
    return video_info



def recieve_videoIds(youtube,playlist_id):

     request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50)
     response = request.execute()
     videoIds=[]
     for i in range(len(response["items"])):
        videoIds.append(response["items"][i]["contentDetails"]["videoId"])

     next_page_token = response.get("nextPageToken")
     more_pages = True

     while more_pages:
        if next_page_token is None:
          more_pages = False
        else:
           request = youtube.playlistItems().list(
              part = "contentDetails",
              playlistId = playlist_id,
              maxResults = 50,
              pageToken = next_page_token)
           response = request.execute()
           for i in range(len(response["items"])):
              videoIds.append(response["items"][i]["contentDetails"]["videoId"])

           next_page_token = response.get("nextPageToken")

     return videoIds



def get_comments_in_videos(youtube, video_id):
    all_comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()

        for item in response['items']:
            data={'comment_id':item['snippet']['topLevelComment']['id'],
                  'comment_txt':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                  'videoId':item['snippet']['topLevelComment']["snippet"]['videoId'],
                  'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  'published_at':item['snippet']['topLevelComment']['snippet']['publishedAt'],
            }


            all_comments.append(data)

    except:
        return 'Could not get comments for video '
    
    return all_comments


LittleBoy=pymongo.MongoClient("mongodb://ganesh:<password>@ac-ugrxwba-shard-00-00.57bfdah.mongodb.net:27017,ac-ugrxwba-shard-00-01.57bfdah.mongodb.net:27017,ac-ugrxwba-shard-00-02.57bfdah.mongodb.net:27017/?ssl=true&replicaSet=atlas-5wai6n-shard-0&authSource=admin&retryWrites=true&w=majority") # enter your connection and password

db=LittleBoy["project"]

@st.cache_data

def channel_Details(channel_id):
  det=recieve_channel_stat(youtube,channel_id)
  col=db["channels"]
  col.insert_one(det)
  playlist=recieve_playlists(youtube,channel_id)
  col=db["playlists"]
  for i in playlist:
    col.insert_one(i)
  Playlist=det.get('playlistId')
  videos=recieve_videoIds(youtube, Playlist)
  for i in videos:
    v=get_video_details(youtube, i)
    col=db["videos"]
    col.insert_one(v)
    c=get_comments_in_videos(youtube, i)
    if c!='Could not get comments for video ':
      for j in c:
        col=db["comments"]
        col.insert_one(j)
  return ("process for" + channel_id +"is completed")



host = 'localhost'
port = '5432'
database = 'project_1'
username = 'postgres'
password = 'enter your password'

# Establish the connection
PROJECT = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)

cursor = PROJECT.cursor()

def channels_box():
    
    try:
        cursor.execute('''create table if not exists channels(channelName varchar(50), 
                       channelId varchar(50), 
                       subscriptionCount bigint, 
                       channelViews bigint, 
                       totalVideos int, 
                       channelDescription text, 
                       playlistId varchar(50), 
                       primary key(channelId))''')
        PROJECT.commit()
    except:
        PROJECT.rollback()
    col1=db["channels"]
    doc = col1.find()
    data = list(doc)
    YT = pd.DataFrame(data)
    try:
        for _, row in YT.iterrows():
            insert_query = '''
                 INSERT INTO channels (channelName, channelId, subscriptionCount, channelViews, totalVideos, channelDescription, playlistId)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['channelName'],
                row['channelId'],
                row['subscriptionCount'],
                row['channelViews'],
                row['totalVideos'],
                row['channelDescription'],
                row['playlistId']
            )
            try:
                cursor.execute(insert_query, values)
                PROJECT.commit()
            except:
                PROJECT.rollback()
    except:
        st.write("values already exists in the channel")

def playlists_box():
    
    try:
        cursor.execute('''create table if not exists playlists(PlaylistId varchar(50), 
                        Title text, 
                        ChannelId varchar(50), 
                        channelName varchar(50), 
                        PublishedAt timestamp, 
                        VideoCount int, 
                        primary key(PlaylistId))''')
        PROJECT.commit()
    except:
        PROJECT.rollback()
    col2=db["playlists"]
    doc2=col2.find()
    data2=list(doc2)
    YT2=pd.DataFrame(data2)
    try:
        for _, row in YT2.iterrows():
            insert_query = '''
                INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'],
            )
            try:
                cursor.execute(insert_query, values)
                PROJECT.commit()
            except:
                PROJECT.rollback()
    except:
        st.write("values already exists in the playlist table")
        

def videos_box():
    try:
        cursor.execute('''create table if not exists videos(video_id varchar(50), 
                          channelTitle varchar(150), 
                          title text, 
                          description text, 
                          tags text, 
                          publishedAt timestamp, 
                          channelId varchar(50), 
                          viewCount bigint, 
                          likeCount bigint, 
                          favouriteCount int, 
                          commentCount int, 
                          duration varchar(10), 
                          definition varchar(150), 
                          caption text, 
                          primary key(video_id))''')
        PROJECT.commit()
    except:
        PROJECT.rollback()
    col3=db["videos"]
    doc3=col3.find()
    data3=list(doc3)
    YT3=pd.DataFrame(data3)
    try:
        for _, row in YT3.iterrows():
            insert_query = '''
                INSERT INTO videos (video_id, channelTitle, title, description, tags, publishedAt,channelId, viewCount,\
                likeCount, favouriteCount, commentCount, duration, definition, caption)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['video_id'],
                row['channelTitle'],
                row['title'],
                row['description'],
                row['tags'],
                row['publishedAt'],
                row['channelId'],
                row['viewCount'],
                row['likeCount'],
                row['favouriteCount'],
                row['commentCount'],
                row['duration'],
                row['definition'],
                row['caption'],
            )
            try:
                cursor.execute(insert_query, values)
                PROJECT.commit()
            except:
                PROJECT.rollback()
    except:
        st.write("values already exists in the video table")


def comments_box():
    try:
        cursor.execute('''create table if not exists comments(comment_id varchar(50), 
                       comment_txt text, 
                       videoId varchar(50), 
                       author_name text, 
                       published_at timestamp, 
                       primary key(comment_id))''')
        PROJECT.commit()
    except:
        PROJECT.rollback()
    col4=db["comments"]
    doc4=col4.find()
    data4=list(doc4)
    YT4=pd.DataFrame(data4)
    try:
        for _, row in YT4.iterrows():
            insert_query = '''
                INSERT INTO comments (comment_id, comment_txt, videoId, author_name, published_at)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['comment_id'],
                row['comment_txt'],
                row['videoId'],
                row['author_name'],
                row['published_at'],
            )
            try:
                cursor.execute(insert_query, values)
                PROJECT.commit()
            except:
                PROJECT.rollback()
    except:
        st.write("values already exists in the comments table")

def box():
    channels_box()
    playlists_box()
    videos_box()
    comments_box()
    return("done")

def show_channels():
    try:
        cursor.execute("select * from channels;")
        boxofchannels=cursor.fetchall()
        boxofchannels=st.dataframe(boxofchannels)
        return boxofchannels
    except:
        PROJECT.rollback()
        cursor.execute("select * from channels;")
        boxofchannels=cursor.fetchall()
        boxofchannels=st.dataframe(boxofchannels)
        return boxofchannels
        
        
        
def show_playlists():
    try:
        cursor.execute("select * from playlists;")
        boxofplaylists=cursor.fetchall()
        boxofplaylists=st.dataframe(boxofplaylists)
        return boxofplaylists
    except:
        PROJECT.rollback()
        cursor.execute("select * from playlists;")
        boxofplaylists=cursor.fetchall()
        boxofplaylists=st.dataframe(boxofplaylists)
        return boxofplaylists
        

def show_videos():
    try:
        cursor.execute("select * from videos;")
        boxofvideos=cursor.fetchall()
        boxofvideos=st.dataframe(boxofvideos)
        return boxofvideos
    except:
        PROJECT.rollback()
        cursor.execute("select * from videos;")
        boxofvideos=cursor.fetchall()
        boxofvideos=st.dataframe(boxofvideos)
        return boxofvideos
        

def show_comments():
    try:
        cursor.execute("select * from comments;")
        boxofcomments=cursor.fetchall()
        boxofcomments=st.dataframe(boxofcomments)
        return boxofcomments
    except:
        PROJECT.rollback()
        cursor.execute("select * from comments;")
        boxofcomments=cursor.fetchall()
        boxofcomments=st.dataframe(boxofcomments)
        return boxofcomments
        


def one():
    cursor.execute("select title as videos, channelTitle as channelName from videos;")
    PROJECT.commit()
    A=cursor.fetchall()
    A=st.dataframe(A)
    return A


def two():
    cursor.execute("select channelName, totalVideos from channels order by totalVideos desc;")
    PROJECT.commit()
    B=cursor.fetchall()
    B=st.dataframe(B)
    return B


def three():
    cursor.execute("select viewCount, channelTitle, title from videos where viewCount is not null order by viewCount desc limit 10;")
    PROJECT.commit()
    C=cursor.fetchall()
    C=st.dataframe(C)
    return C


def four():
    cursor.execute("select commentCount,title from videos where commentCount is not null;")
    PROJECT.commit()
    D=cursor.fetchall()
    D=st.dataframe(D)
    return D


def five():
    cursor.execute("select title, channelTitle, likeCount from videos where likeCount is not null order by likeCount desc;")
    PROJECT.commit()
    E=cursor.fetchall()
    E=st.dataframe(E)
    return E


def six():
    cursor.execute("select channelTitle, title, likeCount from videos where likeCount is not null order by likeCount desc;")
    PROJECT.commit()
    F=cursor.fetchall()
    F=st.dataframe(F)
    return F



def seven():
    cursor.execute("select channelName, channelViews from channels;")
    PROJECT.commit()
    G=cursor.fetchall()
    G=st.dataframe(G)
    return G


def eight():
    cursor.execute("select title, publishedAt, channelTitle from videos where extract(year from publishedAt) = 2022;")
    PROJECT.commit()
    H=cursor.fetchall()
    H=st.dataframe(H)
    return H


def nine():
    cursor.execute("select title, channelTitle, commentCount from videos where commentCount is not null order by commentCount desc;")
    PROJECT.commit()
    I=cursor.fetchall()
    I=st.dataframe(I)
    return I



st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
st.caption("Getting details of the channel")

options = st.multiselect(
    'Choose the channel',
    [Wisdom_Frames, Sp_Star_Entertainment, Focus_Academy_Lectures, Sathya_Jyothi_Films, Vetrii_IAS_Study_Circle, Aravind_SA,                      Flyingmeenaboi, All_About_Mechanical_Engineering, HiphopTamizha, Top_Media],
    [])

st.write('You choosed:', options)

if st.button("Get and Store"):
    for i in options:
        output=channel_Details(i)
        st.write(output)

st.write("Click to migrate data")
if st.button("Migrate"):
    show=box()
    st.write(show)
frames = st.radio(
    "Choose the table you want to view",
    ('None', 'Channel', 'Playlist', 'Video', 'Comment'))

st.write('You choosed :', frames)

if frames=='None':
     st.write("Choose a table")
elif frames=='Channel':
    show_channels()
elif frames=='Playlist':
    show_playlists()
elif frames=='Video':
    show_videos()
elif frames=='Comment':
    show_comments()

    
    
query = st.radio(
    'Youtube Datas',
    ('None', 'Names of videos and channels', 'Channel with most number of videos', 'Top 10 most viewed videos', 'Comments on each video',
     'Highest number of likes', 'Likes and dislikes of all video', 'Total views of each channel','Videos published in the year 2022',
     'High commented videos'))

if query=='None':
    st.write("None")
if query=='Names of videos and channels':
    one()
elif query=='Channel with most number of videos':
    two()
elif query=='Top 10 most viewed videos':
    three()
elif query=='Comments on each video':
    four()
elif query=='Highest number of likes':
    five()
elif query=='Likes and dislikes of all video':
    six()
elif query=='Total views of each channel':
    seven()
elif query=='Videos published in the year 2022':
    eight()
elif query=='High commented videos':
    nine()
      

    
































