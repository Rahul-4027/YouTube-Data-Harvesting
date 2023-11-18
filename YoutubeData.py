import streamlit as st
import re
import pymongo
import mysql.connector
from googleapiclient.discovery import build
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


#Youtube API Connection:
def Connect_Api():
    API_key= "AIzaSyCrwu_DU4DDpIJFqgQWHIU_W3YwCxTfMEU"

    API_Service ="youtube"
    API_Version  ="v3"

    Youtube_connect =build(API_Service,API_Version,developerKey=API_key)
    return Youtube_connect

youtube_conn = Connect_Api()

#Getting channel Information
def Channel_info(channel_id):
    request = youtube_conn.channels().list(
            part= "snippet,contentDetails,Statistics",
            id=channel_id
    )

    response =request.execute()

    for i in response['items']:
        data= dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscription_Count=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos = i["statistics"]["videoCount"],
                Channel_Description = i["snippet"]["description"],
                Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data               

#Getting video ids details:
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube_conn.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube_conn.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

#Getting video Information:
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube_conn.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
             data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
             video_data.append(data)
    return video_data

#Getting Comment informaton
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube_conn.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information

#MongoDB Connection
client = pymongo.MongoClient("mongodb+srv://rahulparthiban743:ue7nSJJuMkoC96dm@cluster0.9yprt.mongodb.net/")
db = client["Youtube_data"]

#Store collected information into MongoDB
def channel_details1(channel_id):
   Ch_details= Channel_info(channel_id)
   vi_ids    =get_channel_videos(channel_id)
   vi_details= get_video_info(vi_ids)
   cm_details=get_comment_info(vi_ids)
   coll1=db["channel_details1"]
   coll1.insert_one({"channel_information":Ch_details,"video_information":vi_details,"comment_information":cm_details})
   
   return "Updated successfully"

# MySQL configuration
def channel_table():
    config = {
        'user': 'root',
        'password': 'Rahul@555',
        'host': 'localhost',
        'database': 'youtube_data',
        'auth_plugin': 'mysql_native_password',
    }

    # Establish the connection
    mydb_connection = mysql.connector.connect(**config)
    cursor = mydb_connection.cursor()

    # Drop and create query for channels  table
    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    mydb_connection.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels (
                            Channel_Name VARCHAR(100),
                            Channel_Id VARCHAR(80) PRIMARY KEY,
                            Subscription_Count BIGINT,
                            Views BIGINT,
                            Total_Videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(50)
                        )'''
        cursor.execute(create_query)
        mydb_connection.commit()
    except:
        st.write("Channels Table already created") 

    #Fetch data from MongoDB
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details1"]

    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels (
                            Channel_Name,
                            Channel_Id,
                            Subscription_Count,
                            Views,
                            Total_Videos,
                            Channel_Description,
                            Playlist_Id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id']
        )

        try:
            cursor.execute(insert_query, values)
            mydb_connection.commit()
        except:
            st.write("Channels values are already inserted")

def video_table():
    config = {
            'user': 'root',
            'password': 'Rahul@555',
            'host': 'localhost',
            'database': 'youtube_data',
            'auth_plugin': 'mysql_native_password',
        }

        # Establish the connection
    mydb_connection = mysql.connector.connect(**config)
    cursor = mydb_connection.cursor()

        # Drop and create the table
    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb_connection.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            Channel_Name VARCHAR(250),
                            Channel_Id VARCHAR(100),
                            Video_Id VARCHAR(50) PRIMARY KEY,
                            Title VARCHAR(250),
                            Tags TEXT,
                            Thumbnail VARCHAR(250),
                            Description TEXT,
                            Published_Date TIMESTAMP,
                            Duration BIGINT, 
                            Views BIGINT,
                            Likes BIGINT,
                            Comments INT,
                            Favorite_Count INT,
                            Definition VARCHAR(100),
                            Caption_Status VARCHAR(150)
                        )'''
                            
        cursor.execute(create_query)
        mydb_connection.commit()
    except:
        st.write("Videos Table alrady created")

    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details1"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    #Converting ISO 8601 Interval format to seconds for MYSQL compatiblity
    def iso8601_duration_to_hms(duration):
        # Regular expression to match ISO 8601 duration format
        duration_regex = re.compile(r'^P(?:\d+Y)?(?:\d+M)?(?:\d+D)?T?(?:\d+H)?(?:\d+M)?(?:\d+S)?$')

        if not duration_regex.match(duration):
            raise ValueError("Invalid ISO 8601 duration format")

        years = re.search(r'(\d+)Y', duration)
        months = re.search(r'(\d+)M', duration)
        days = re.search(r'(\d+)D', duration)
        hours = re.search(r'(\d+)H', duration)
        minutes = re.search(r'(\d+)M', duration)
        seconds = re.search(r'(\d+)S', duration)

        total_seconds = 0

        if years:
            total_seconds += int(years.group(1)) * 365 * 24 * 60 * 60
        if months:
            total_seconds += int(months.group(1)) * 30 * 24 * 60 * 60
        if days:
            total_seconds += int(days.group(1)) * 24 * 60 * 60
        if hours:
            total_seconds += int(hours.group(1)) * 60 * 60
        if minutes:
            total_seconds += int(minutes.group(1)) * 60
        if seconds:
            total_seconds += int(seconds.group(1))

        return total_seconds
        

    for index, row in df2.iterrows():
            insert_query = '''
                        INSERT INTO videos (Channel_Name,
                            Channel_Id,
                            Video_Id, 
                            Title, 
                            Tags,
                            Thumbnail,
                            Description, 
                            Published_Date,
                            Duration, 
                            Views, 
                            Likes,
                            Comments,
                            Favorite_Count, 
                            Definition, 
                            Caption_Status 
                            )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                    '''
            # Convert 'Tags' to a string if it's not already a list
            tags_str = ','.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']

            # Convert the string to a datetime object
            published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            # Converted interval storing in duartion_seconds
            duration_seconds = iso8601_duration_to_hms(row['Duration'])
            values = (
                        row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        tags_str,
                        row['Thumbnail'],
                        row['Description'],
                        published_date,
                        duration_seconds,
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])
                                    
            try:    
                cursor.execute(insert_query,values)
                mydb_connection.commit()
            except:
                st.write("videos values already inserted in the table")
        
def comments_table():
    config = {
        'user': 'root',
        'password': 'Rahul@555',
        'host': 'localhost',
        'database': 'youtube_data',
        'auth_plugin': 'mysql_native_password',
    }

    # Establish the connection
    mydb_connection = mysql.connector.connect(**config)
    cursor = mydb_connection.cursor()

    # Drop and create the table
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb_connection.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
            Comment_Id VARCHAR(100) PRIMARY KEY,
            Video_Id VARCHAR(80),
            Comment_Text TEXT,
            Comment_Author VARCHAR(150),
            Comment_Published TIMESTAMP)'''
                        
        cursor.execute(create_query)
        mydb_connection.commit()
    except:
        st.write("Comments Table already created")

    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details1"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
            VALUES (%s, %s, %s, %s, %s)
        '''
        
        # Convert 'Comment_Published' to MySQL-compatible datetime format
        comment_published = datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ')

        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            comment_published
        )
            
        try:
                    cursor.execute(insert_query,values)
                    mydb_connection.commit()
        except:
                st.write("This comments are already exist in comments table")
            
def tables():
    channel_table()
    video_table()
    comments_table()
    return "Tables Created successfully"

def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details1"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details1"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details1"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details1"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            print("already exists")
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details1(channel)
            st.success(output)

if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()

#MYSQl Connection 
config = {
            'user': 'root',
            'password': 'Rahul@555',
            'host': 'localhost',
            'database': 'youtube_data',
            'auth_plugin': 'mysql_native_password',
        }

        # Establish the connection
mydb_connection = mysql.connector.connect(**config)
cursor = mydb_connection.cursor()

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))