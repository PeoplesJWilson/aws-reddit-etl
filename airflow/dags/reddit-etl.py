from airflow.decorators import dag,task
import config
from datetime import date,datetime
import mysql.connector
import pendulum
import requests



subreddit_name_1 = "math"
subreddit_name_2 = "dataengineering"

@dag(
    dag_id="reddit_pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023,7,11),
    catchup=False 
)

def reddit_pipeline():

    @task()
    def create_table():
        connection = mysql.connector.connect(
        host = config.rds_endpoint,
        user = config.rds_user,
        password = config.rds_password,
        database = config.rds_db_name

        )
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS posts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                subreddit VARCHAR(25),
                title VARCHAR(255),
                user VARCHAR(25),
                content TEXT,
                upvotes INT,
                created_at DATETIME,
                num_comments INT
            )
            """
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()


    @task()
    def get_subreddit_posts(subreddit):
        auth = requests.auth.HTTPBasicAuth(config.api_key, config.api_secret)

        data = {'grant_type': 'password',
                'username': config.username,
                'password': config.password}

        headers = {'User-Agent': config.botname}

        response = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=data, headers=headers)
        
        TOKEN = response.json()['access_token']
        headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
        response = requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)

        print(f"printing response for api call to r/{subreddit}:")
        print(response)


        params = {"t":"day"}
        get_url = f"https://oauth.reddit.com/r/{subreddit}"
        response = requests.get(get_url, params = params, headers = headers)

        subreddit_data = response.json()
        subreddit_data = subreddit_data['data']['children']

        posts = []
        for i in range(len(subreddit_data)):
            title = subreddit_data[i]["data"]["title"].lower()
            user = subreddit_data[i]["data"]["author"]
            content = subreddit_data[i]["data"]["selftext"].lower().replace("\n","")
            upvotes = subreddit_data[i]["data"]["ups"]

            timestamp = subreddit_data[i]["data"]["created_utc"]
            time = datetime.fromtimestamp(timestamp)

            num_comments = subreddit_data[i]["data"]["num_comments"]

            posts.append([(subreddit,
                        title, 
                        user, 
                        content, 
                        upvotes, 
                        time, 
                        num_comments)])

        
        return posts
    


    @task()
    def insert_into_posts(posts):

        connection = mysql.connector.connect(
        host = config.rds_endpoint,
        user = config.rds_user,
        password = config.rds_password,
        database = config.rds_db_name
        )

        cursor = connection.cursor()

        insert_query = """INSERT INTO posts (
            subreddit, 
            title, 
            user, 
            content, 
            upvotes, 
            created_at, 
            num_comments
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        
        formatted_posts = []
        for post in posts:
            formatted_posts.extend(post)
        
        cursor.executemany(insert_query, formatted_posts)
        connection.commit()
        cursor.close()
        connection.close()
    


    task_name_1 = f"api_call_to_r_{subreddit_name_1}"
    task_name_2 = f"api_call_to_r_{subreddit_name_2}"


    create_table_task = create_table()

    posts_1 = get_subreddit_posts.override(task_id=task_name_1)(subreddit_name_1)
    posts_2 = get_subreddit_posts.override(task_id=task_name_2)(subreddit_name_2)

    insert_task_1 = insert_into_posts.override(task_id = f"insert_items_from_{subreddit_name_1}")(posts_1)
    insert_task_2 = insert_into_posts.override(task_id = f"insert_items_from_{subreddit_name_2}")(posts_2)



    create_table_task >> posts_1
    create_table_task >> posts_2

    posts_1 >> insert_task_1
    posts_2 >> insert_task_2




pipeline = reddit_pipeline()