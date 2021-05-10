"""
YouTube Data Collector
Collects comprehensive analytics data from YouTube Data API v3
"""

import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import time
import json

class YouTubeCollector:
    """YouTube Data API collector for content analytics"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        
        # Rate limiting
        self.requests_per_day = 10000
        self.requests_made = 0
        self.last_reset = datetime.now()
    
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Make API request with rate limiting and error handling"""
        
        # Check rate limit
        if self._check_rate_limit():
            time.sleep(1)  # Brief pause if approaching limit
        
        params['key'] = self.api_key
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            self.requests_made += 1
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"YouTube API request failed: {e}")
            raise
    
    def _check_rate_limit(self) -> bool:
        """Check if approaching rate limit"""
        now = datetime.now()
        
        # Reset counter daily
        if (now - self.last_reset).days >= 1:
            self.requests_made = 0
            self.last_reset = now
        
        return self.requests_made >= self.requests_per_day * 0.9
    
    def get_channel_analytics(self, channel_id: str) -> Dict:
        """Get comprehensive channel analytics"""
        
        # Get channel details
        channel_params = {
            'part': 'snippet,statistics,brandingSettings,contentDetails',
            'id': channel_id
        }
        
        channel_data = self._make_request('channels', channel_params)
        
        if not channel_data.get('items'):
            self.logger.warning(f"No data found for channel: {channel_id}")
            return {}
        
        channel_info = channel_data['items'][0]
        
        # Extract analytics data
        analytics = {
            'id': f"channel_{channel_id}_{datetime.now().strftime('%Y%m%d')}",
            'channel_id': channel_id,
            'video_id': None,
            'title': channel_info['snippet']['title'],
            'description': channel_info['snippet']['description'],
            'published_at': channel_info['snippet']['publishedAt'],
            'view_count': int(channel_info['statistics'].get('viewCount', 0)),
            'like_count': None,
            'comment_count': int(channel_info['statistics'].get('commentCount', 0)),
            'subscriber_count': int(channel_info['statistics'].get('subscriberCount', 0)),
            'duration': None,
            'tags': [],
            'category_id': None,
            'language': channel_info['snippet'].get('defaultLanguage', 'unknown'),
            'collected_at': datetime.now().isoformat(),
            'data_type': 'channel',
            'video_count': int(channel_info['statistics'].get('videoCount', 0)),
            'country': channel_info['snippet'].get('country'),
            'custom_url': channel_info['snippet'].get('customUrl'),
            'thumbnail_url': channel_info['snippet']['thumbnails']['high']['url']
        }
        
        return analytics
    
    def get_channel_videos(self, channel_id: str, max_results: int = 50) -> List[Dict]:
        """Get recent videos from a channel"""
        
        # Get uploads playlist ID
        channel_params = {
            'part': 'contentDetails',
            'id': channel_id
        }
        
        channel_data = self._make_request('channels', channel_params)
        
        if not channel_data.get('items'):
            return []
        
        uploads_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Get videos from uploads playlist
        playlist_params = {
            'part': 'snippet',
            'playlistId': uploads_playlist_id,
            'maxResults': min(max_results, 50)
        }
        
        playlist_data = self._make_request('playlistItems', playlist_params)
        
        videos = []
        for item in playlist_data.get('items', []):
            video_info = {
                'id': item['snippet']['resourceId']['videoId'],
                'title': item['snippet']['title'],
                'published_at': item['snippet']['publishedAt'],
                'channel_id': channel_id
            }
            videos.append(video_info)
        
        return videos
    
    def get_video_analytics(self, video_id: str) -> Dict:
        """Get comprehensive video analytics"""
        
        # Get video details and statistics
        video_params = {
            'part': 'snippet,statistics,contentDetails,status',
            'id': video_id
        }
        
        video_data = self._make_request('videos', video_params)
        
        if not video_data.get('items'):
            self.logger.warning(f"No data found for video: {video_id}")
            return {}
        
        video_info = video_data['items'][0]
        
        # Parse duration (PT4M13S format)
        duration_str = video_info['contentDetails']['duration']
        duration_seconds = self._parse_duration(duration_str)
        
        # Extract analytics data
        analytics = {
            'id': f"video_{video_id}_{datetime.now().strftime('%Y%m%d')}",
            'channel_id': video_info['snippet']['channelId'],
            'video_id': video_id,
            'title': video_info['snippet']['title'],
            'description': video_info['snippet']['description'],
            'published_at': video_info['snippet']['publishedAt'],
            'view_count': int(video_info['statistics'].get('viewCount', 0)),
            'like_count': int(video_info['statistics'].get('likeCount', 0)),
            'comment_count': int(video_info['statistics'].get('commentCount', 0)),
            'subscriber_count': None,
            'duration': duration_seconds,
            'tags': video_info['snippet'].get('tags', []),
            'category_id': int(video_info['snippet'].get('categoryId', 0)),
            'language': video_info['snippet'].get('defaultLanguage', 'unknown'),
            'collected_at': datetime.now().isoformat(),
            'data_type': 'video',
            'thumbnail_url': video_info['snippet']['thumbnails']['high']['url'],
            'privacy_status': video_info['status']['privacyStatus'],
            'made_for_kids': video_info['status'].get('madeForKids', False)
        }
        
        return analytics
    
    def _parse_duration(self, duration_str: str) -> int:
        """Parse YouTube duration format (PT4M13S) to seconds"""
        import re
        
        pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
        match = re.match(pattern, duration_str)
        
        if not match:
            return 0
        
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        
        return hours * 3600 + minutes * 60 + seconds
    
    def get_trending_videos(self, region_code: str = 'US', max_results: int = 50) -> List[Dict]:
        """Get trending videos for a region"""
        
        params = {
            'part': 'snippet,statistics',
            'chart': 'mostPopular',
            'regionCode': region_code,
            'maxResults': min(max_results, 50)
        }
        
        trending_data = self._make_request('videos', params)
        
        trending_videos = []
        for item in trending_data.get('items', []):
            video_analytics = {
                'id': f"trending_{item['id']}_{datetime.now().strftime('%Y%m%d')}",
                'channel_id': item['snippet']['channelId'],
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'published_at': item['snippet']['publishedAt'],
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'comment_count': int(item['statistics'].get('commentCount', 0)),
                'subscriber_count': None,
                'duration': None,
                'tags': item['snippet'].get('tags', []),
                'category_id': int(item['snippet'].get('categoryId', 0)),
                'language': item['snippet'].get('defaultLanguage', 'unknown'),
                'collected_at': datetime.now().isoformat(),
                'data_type': 'trending',
                'region_code': region_code,
                'thumbnail_url': item['snippet']['thumbnails']['high']['url']
            }
            trending_videos.append(video_analytics)
        
        return trending_videos
    
    def search_videos(self, query: str, max_results: int = 50, 
                     published_after: Optional[datetime] = None) -> List[Dict]:
        """Search for videos with specific criteria"""
        
        params = {
            'part': 'snippet',
            'q': query,
            'type': 'video',
            'maxResults': min(max_results, 50),
            'order': 'relevance'
        }
        
        if published_after:
            params['publishedAfter'] = published_after.isoformat() + 'Z'
        
        search_data = self._make_request('search', params)
        
        videos = []
        for item in search_data.get('items', []):
            video_info = {
                'id': item['id']['videoId'],
                'title': item['snippet']['title'],
                'channel_id': item['snippet']['channelId'],
                'published_at': item['snippet']['publishedAt'],
                'description': item['snippet']['description'],
                'thumbnail_url': item['snippet']['thumbnails']['high']['url']
            }
            videos.append(video_info)
        
        return videos
    
    def get_video_comments(self, video_id: str, max_results: int = 100) -> List[Dict]:
        """Get comments for a video"""
        
        params = {
            'part': 'snippet,replies',
            'videoId': video_id,
            'maxResults': min(max_results, 100),
            'order': 'relevance'
        }
        
        try:
            comments_data = self._make_request('commentThreads', params)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.logger.warning(f"Comments disabled for video: {video_id}")
                return []
            raise
        
        comments = []
        for item in comments_data.get('items', []):
            comment_info = {
                'id': item['id'],
                'video_id': video_id,
                'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'published_at': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                'like_count': item['snippet']['topLevelComment']['snippet']['likeCount'],
                'reply_count': item['snippet']['totalReplyCount'],
                'collected_at': datetime.now().isoformat()
            }
            comments.append(comment_info)
        
        return comments
    
    def get_channel_playlists(self, channel_id: str) -> List[Dict]:
        """Get playlists for a channel"""
        
        params = {
            'part': 'snippet,contentDetails',
            'channelId': channel_id,
            'maxResults': 50
        }
        
        playlists_data = self._make_request('playlists', params)
        
        playlists = []
        for item in playlists_data.get('items', []):
            playlist_info = {
                'id': item['id'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'channel_id': channel_id,
                'published_at': item['snippet']['publishedAt'],
                'item_count': item['contentDetails']['itemCount'],
                'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
                'collected_at': datetime.now().isoformat()
            }
            playlists.append(playlist_info)
        
        return playlists
    
    def get_category_analytics(self, region_code: str = 'US') -> List[Dict]:
        """Get video categories and their analytics"""
        
        params = {
            'part': 'snippet',
            'regionCode': region_code
        }
        
        categories_data = self._make_request('videoCategories', params)
        
        categories = []
        for item in categories_data.get('items', []):
            category_info = {
                'id': item['id'],
                'title': item['snippet']['title'],
                'channel_id': item['snippet']['channelId'],
                'region_code': region_code,
                'collected_at': datetime.now().isoformat()
            }
            categories.append(category_info)
        
        return categories
    
    def get_channel_sections(self, channel_id: str) -> List[Dict]:
        """Get channel sections (featured content, playlists, etc.)"""
        
        params = {
            'part': 'snippet,contentDetails',
            'channelId': channel_id
        }
        
        sections_data = self._make_request('channelSections', params)
        
        sections = []
        for item in sections_data.get('items', []):
            section_info = {
                'id': item['id'],
                'type': item['snippet']['type'],
                'title': item['snippet'].get('title', ''),
                'channel_id': channel_id,
                'position': item['snippet']['position'],
                'collected_at': datetime.now().isoformat()
            }
            sections.append(section_info)
        
        return sections

def collect_comprehensive_youtube_data(api_key: str, channel_ids: List[str]) -> Dict:
    """Collect comprehensive YouTube data for multiple channels"""
    
    collector = YouTubeCollector(api_key)
    all_data = {
        'channels': [],
        'videos': [],
        'comments': [],
        'playlists': [],
        'trending': []
    }
    
    # Collect trending videos
    try:
        trending_videos = collector.get_trending_videos()
        all_data['trending'].extend(trending_videos)
    except Exception as e:
        logging.error(f"Error collecting trending videos: {e}")
    
    # Collect data for each channel
    for channel_id in channel_ids:
        try:
            # Channel analytics
            channel_data = collector.get_channel_analytics(channel_id)
            if channel_data:
                all_data['channels'].append(channel_data)
            
            # Recent videos
            videos = collector.get_channel_videos(channel_id, max_results=20)
            for video in videos:
                video_analytics = collector.get_video_analytics(video['id'])
                if video_analytics:
                    all_data['videos'].append(video_analytics)
                
                # Comments for each video (limited to avoid quota issues)
                comments = collector.get_video_comments(video['id'], max_results=10)
                all_data['comments'].extend(comments)
            
            # Channel playlists
            playlists = collector.get_channel_playlists(channel_id)
            all_data['playlists'].extend(playlists)
            
            logging.info(f"Collected data for YouTube channel: {channel_id}")
            
        except Exception as e:
            logging.error(f"Error collecting data for channel {channel_id}: {e}")
    
    return all_data

if __name__ == "__main__":
    # Example usage
    import os
    
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("Please set YOUTUBE_API_KEY environment variable")
        exit(1)
    
    # Example channel IDs (replace with actual channels)
    channel_ids = [
        "UCBJycsmduvYEL83R_U4JriQ",  # Marques Brownlee
        "UCsooa4yRKGN_zEE8iknghZA"   # TED-Ed
    ]
    
    data = collect_comprehensive_youtube_data(api_key, channel_ids)
    
    print(f"Collected:")
    print(f"- {len(data['channels'])} channels")
    print(f"- {len(data['videos'])} videos")
    print(f"- {len(data['comments'])} comments")
    print(f"- {len(data['playlists'])} playlists")
    print(f"- {len(data['trending'])} trending videos")