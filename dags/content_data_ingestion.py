"""
ContentFlow Analytics Studio - Content Data Ingestion DAG
Orchestrates data collection from multiple content platforms
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
import logging
import json
import os

# DAG Configuration
default_args = {
    'owner': 'content-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 2
}

dag = DAG(
    'content_data_ingestion',
    default_args=default_args,
    description='Content data ingestion from multiple platforms',
    schedule_interval='@hourly',
    catchup=False,
    tags=['content', 'analytics', 'ingestion']
)

def collect_youtube_data(**context):
    """Collect data from YouTube Data API"""
    from scripts.data_collectors.youtube_collector import YouTubeCollector
    
    api_key = Variable.get("YOUTUBE_API_KEY")
    collector = YouTubeCollector(api_key)
    
    # Get channel IDs to monitor
    channel_ids = Variable.get("YOUTUBE_CHANNEL_IDS", "").split(",")
    
    collected_data = []
    for channel_id in channel_ids:
        if channel_id.strip():
            try:
                # Collect channel data
                channel_data = collector.get_channel_analytics(channel_id.strip())
                collected_data.append(channel_data)
                
                # Collect recent videos
                videos = collector.get_channel_videos(channel_id.strip(), max_results=50)
                for video in videos:
                    video_analytics = collector.get_video_analytics(video['id'])
                    collected_data.append(video_analytics)
                
                logging.info(f"Collected data for YouTube channel: {channel_id}")
                
            except Exception as e:
                logging.error(f"Error collecting YouTube data for {channel_id}: {str(e)}")
    
    # Save to temporary file
    output_file = f"/tmp/youtube_data_{context['ds']}.json"
    with open(output_file, 'w') as f:
        json.dump(collected_data, f, indent=2)
    
    return output_file

def collect_spotify_data(**context):
    """Collect data from Spotify Web API"""
    from scripts.data_collectors.spotify_collector import SpotifyCollector
    
    client_id = Variable.get("SPOTIFY_CLIENT_ID")
    client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")
    collector = SpotifyCollector(client_id, client_secret)
    
    # Get artist IDs to monitor
    artist_ids = Variable.get("SPOTIFY_ARTIST_IDS", "").split(",")
    
    collected_data = []
    for artist_id in artist_ids:
        if artist_id.strip():
            try:
                # Collect artist data
                artist_data = collector.get_artist_analytics(artist_id.strip())
                collected_data.append(artist_data)
                
                # Collect albums and tracks
                albums = collector.get_artist_albums(artist_id.strip())
                for album in albums:
                    album_analytics = collector.get_album_analytics(album['id'])
                    collected_data.append(album_analytics)
                
                logging.info(f"Collected data for Spotify artist: {artist_id}")
                
            except Exception as e:
                logging.error(f"Error collecting Spotify data for {artist_id}: {str(e)}")
    
    # Collect playlist data
    playlist_ids = Variable.get("SPOTIFY_PLAYLIST_IDS", "").split(",")
    for playlist_id in playlist_ids:
        if playlist_id.strip():
            try:
                playlist_data = collector.get_playlist_analytics(playlist_id.strip())
                collected_data.append(playlist_data)
            except Exception as e:
                logging.error(f"Error collecting playlist data: {str(e)}")
    
    # Save to temporary file
    output_file = f"/tmp/spotify_data_{context['ds']}.json"
    with open(output_file, 'w') as f:
        json.dump(collected_data, f, indent=2)
    
    return output_file

def collect_social_media_data(**context):
    """Collect data from social media platforms"""
    from scripts.data_collectors.social_media_collector import SocialMediaCollector
    
    # Twitter API credentials
    twitter_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    
    # Instagram credentials
    instagram_access_token = Variable.get("INSTAGRAM_ACCESS_TOKEN")
    
    collector = SocialMediaCollector(
        twitter_bearer_token=twitter_bearer_token,
        instagram_access_token=instagram_access_token
    )
    
    collected_data = []
    
    # Collect Twitter data
    twitter_handles = Variable.get("TWITTER_HANDLES", "").split(",")
    for handle in twitter_handles:
        if handle.strip():
            try:
                twitter_data = collector.get_twitter_analytics(handle.strip())
                collected_data.append(twitter_data)
                logging.info(f"Collected Twitter data for: {handle}")
            except Exception as e:
                logging.error(f"Error collecting Twitter data for {handle}: {str(e)}")
    
    # Collect Instagram data
    instagram_accounts = Variable.get("INSTAGRAM_ACCOUNTS", "").split(",")
    for account in instagram_accounts:
        if account.strip():
            try:
                instagram_data = collector.get_instagram_analytics(account.strip())
                collected_data.append(instagram_data)
                logging.info(f"Collected Instagram data for: {account}")
            except Exception as e:
                logging.error(f"Error collecting Instagram data for {account}: {str(e)}")
    
    # Save to temporary file
    output_file = f"/tmp/social_media_data_{context['ds']}.json"
    with open(output_file, 'w') as f:
        json.dump(collected_data, f, indent=2)
    
    return output_file

def collect_streaming_data(**context):
    """Collect data from streaming platforms"""
    from scripts.data_collectors.netflix_collector import NetflixCollector
    
    # Note: Netflix doesn't have a public API, so this would use web scraping
    # or third-party data sources like JustWatch, TMDB, etc.
    collector = NetflixCollector()
    
    collected_data = []
    
    try:
        # Collect trending content
        trending_data = collector.get_trending_content()
        collected_data.extend(trending_data)
        
        # Collect genre analytics
        genre_data = collector.get_genre_analytics()
        collected_data.extend(genre_data)
        
        logging.info("Collected streaming platform data")
        
    except Exception as e:
        logging.error(f"Error collecting streaming data: {str(e)}")
    
    # Save to temporary file
    output_file = f"/tmp/streaming_data_{context['ds']}.json"
    with open(output_file, 'w') as f:
        json.dump(collected_data, f, indent=2)
    
    return output_file

def process_and_validate_data(**context):
    """Process and validate collected data"""
    from scripts.utils.data_processor import ContentDataProcessor
    
    # Get file paths from previous tasks
    youtube_file = context['task_instance'].xcom_pull(task_ids='collect_youtube_data')
    spotify_file = context['task_instance'].xcom_pull(task_ids='collect_spotify_data')
    social_file = context['task_instance'].xcom_pull(task_ids='collect_social_media_data')
    streaming_file = context['task_instance'].xcom_pull(task_ids='collect_streaming_data')
    
    processor = ContentDataProcessor()
    
    processed_data = {
        'youtube': [],
        'spotify': [],
        'social_media': [],
        'streaming': []
    }
    
    # Process each data source
    for file_path, data_type in [
        (youtube_file, 'youtube'),
        (spotify_file, 'spotify'),
        (social_file, 'social_media'),
        (streaming_file, 'streaming')
    ]:
        if file_path and os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    raw_data = json.load(f)
                
                # Validate and clean data
                cleaned_data = processor.clean_and_validate(raw_data, data_type)
                processed_data[data_type] = cleaned_data
                
                logging.info(f"Processed {len(cleaned_data)} records for {data_type}")
                
            except Exception as e:
                logging.error(f"Error processing {data_type} data: {str(e)}")
    
    # Save processed data
    output_file = f"/tmp/processed_content_data_{context['ds']}.json"
    with open(output_file, 'w') as f:
        json.dump(processed_data, f, indent=2)
    
    return output_file

def load_to_redshift(**context):
    """Load processed data to Redshift"""
    from scripts.utils.redshift_client import RedshiftClient
    
    processed_file = context['task_instance'].xcom_pull(task_ids='process_and_validate_data')
    
    if not processed_file or not os.path.exists(processed_file):
        logging.error("No processed data file found")
        return False
    
    # Initialize Redshift client
    redshift_client = RedshiftClient(
        host=Variable.get("REDSHIFT_HOST"),
        port=Variable.get("REDSHIFT_PORT", 5439),
        database=Variable.get("REDSHIFT_DATABASE"),
        user=Variable.get("REDSHIFT_USER"),
        password=Variable.get("REDSHIFT_PASSWORD")
    )
    
    try:
        with open(processed_file, 'r') as f:
            data = json.load(f)
        
        # Load data to respective tables
        for data_type, records in data.items():
            if records:
                table_name = f"raw_{data_type}_data"
                redshift_client.bulk_insert(table_name, records)
                logging.info(f"Loaded {len(records)} records to {table_name}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error loading data to Redshift: {str(e)}")
        return False

def update_data_quality_metrics(**context):
    """Update data quality metrics"""
    from scripts.utils.data_quality import DataQualityChecker
    
    checker = DataQualityChecker()
    
    # Check data quality for each source
    quality_metrics = {}
    
    data_sources = ['youtube', 'spotify', 'social_media', 'streaming']
    for source in data_sources:
        try:
            metrics = checker.check_data_quality(source, context['ds'])
            quality_metrics[source] = metrics
            logging.info(f"Data quality metrics for {source}: {metrics}")
        except Exception as e:
            logging.error(f"Error checking data quality for {source}: {str(e)}")
    
    # Store metrics in database
    checker.store_quality_metrics(quality_metrics, context['ds'])
    
    return quality_metrics

# Task Definitions
create_tables_task = PostgresOperator(
    task_id='create_database_tables',
    postgres_conn_id='content_postgres',
    sql="""
    -- YouTube data table
    CREATE TABLE IF NOT EXISTS raw_youtube_data (
        id VARCHAR(50) PRIMARY KEY,
        channel_id VARCHAR(50),
        video_id VARCHAR(50),
        title TEXT,
        description TEXT,
        published_at TIMESTAMP,
        view_count BIGINT,
        like_count BIGINT,
        comment_count BIGINT,
        subscriber_count BIGINT,
        duration INTEGER,
        tags TEXT[],
        category_id INTEGER,
        language VARCHAR(10),
        collected_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Spotify data table
    CREATE TABLE IF NOT EXISTS raw_spotify_data (
        id VARCHAR(50) PRIMARY KEY,
        artist_id VARCHAR(50),
        track_id VARCHAR(50),
        album_id VARCHAR(50),
        name TEXT,
        popularity INTEGER,
        followers BIGINT,
        genres TEXT[],
        release_date DATE,
        duration_ms INTEGER,
        explicit BOOLEAN,
        energy FLOAT,
        danceability FLOAT,
        valence FLOAT,
        collected_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Social media data table
    CREATE TABLE IF NOT EXISTS raw_social_media_data (
        id VARCHAR(100) PRIMARY KEY,
        platform VARCHAR(20),
        account_id VARCHAR(50),
        post_id VARCHAR(50),
        content TEXT,
        posted_at TIMESTAMP,
        likes BIGINT,
        shares BIGINT,
        comments BIGINT,
        followers BIGINT,
        engagement_rate FLOAT,
        hashtags TEXT[],
        mentions TEXT[],
        collected_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Streaming data table
    CREATE TABLE IF NOT EXISTS raw_streaming_data (
        id VARCHAR(100) PRIMARY KEY,
        platform VARCHAR(20),
        content_id VARCHAR(50),
        title TEXT,
        genre VARCHAR(50),
        release_date DATE,
        rating FLOAT,
        popularity_score INTEGER,
        country VARCHAR(10),
        content_type VARCHAR(20),
        collected_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Data quality metrics table
    CREATE TABLE IF NOT EXISTS data_quality_metrics (
        id SERIAL PRIMARY KEY,
        data_source VARCHAR(50),
        metric_date DATE,
        total_records INTEGER,
        valid_records INTEGER,
        duplicate_records INTEGER,
        null_values INTEGER,
        quality_score FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """,
    dag=dag
)

collect_youtube_task = PythonOperator(
    task_id='collect_youtube_data',
    python_callable=collect_youtube_data,
    dag=dag
)

collect_spotify_task = PythonOperator(
    task_id='collect_spotify_data',
    python_callable=collect_spotify_data,
    dag=dag
)

collect_social_task = PythonOperator(
    task_id='collect_social_media_data',
    python_callable=collect_social_media_data,
    dag=dag
)

collect_streaming_task = PythonOperator(
    task_id='collect_streaming_data',
    python_callable=collect_streaming_data,
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_and_validate_data',
    python_callable=process_and_validate_data,
    dag=dag
)

# Upload to S3 for backup
upload_to_s3_task = LocalFilesystemToS3Operator(
    task_id='upload_to_s3',
    filename="{{ task_instance.xcom_pull(task_ids='process_and_validate_data') }}",
    dest_key="content-data/{{ ds }}/processed_content_data.json",
    dest_bucket=Variable.get("S3_BUCKET_NAME"),
    aws_conn_id='aws_default',
    replace=True,
    dag=dag
)

load_redshift_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='update_data_quality_metrics',
    python_callable=update_data_quality_metrics,
    dag=dag
)

# Run dbt models
dbt_run_task = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
    cd /opt/airflow/dbt
    dbt run --models staging
    dbt run --models marts
    dbt test
    """,
    dag=dag
)

# Task Dependencies
create_tables_task >> [
    collect_youtube_task,
    collect_spotify_task,
    collect_social_task,
    collect_streaming_task
]

[
    collect_youtube_task,
    collect_spotify_task,
    collect_social_task,
    collect_streaming_task
] >> process_data_task

process_data_task >> [upload_to_s3_task, load_redshift_task]
load_redshift_task >> quality_check_task
quality_check_task >> dbt_run_task