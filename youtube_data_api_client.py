#!/usr/bin/env python

from pathlib import Path
import pandas as pd

import httplib2

from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
YOUTUBE_DATA_CLIENT_SECRETS_FILE = ".top_secrets.json"
YOUTUBE_DATA_API_CLIENT_SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
MISSING_CLIENT_SECRETS_MESSAGE = "Error: %s is not found."

class YoutubeDataApiClient():

    def __init__(self, client_secrets_file, scopes):
        self.__client = self.get_youtube_data_api_client(
            client_secrets_file, scopes)

    def get_youtube_data_api_client(self, client_secrets_file, scopes):
        path = (Path(__file__).parent/client_secrets_file).resolve()
        message = MISSING_CLIENT_SECRETS_MESSAGE % str(path)
        flow = flow_from_clientsecrets(client_secrets_file,
                                        scope=scopes,
                                        message=message)

        storage = Storage('.youtube-oauth2.json')
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            credentials = run_flow(flow, storage)

        return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                        http=credentials.authorize(httplib2.Http()))

    def get_my_channel(self):
        channels = self.__client.channels().list(
            part='snippet,statistics',
            mine=True,
            fields='items(id,snippet(title,description),statistics(videoCount,viewCount,subscriberCount))'
        ).execute()

        channel = channels['items'][0]
        snippet = channel['snippet']
        statistics = channel['statistics']

        return  {
            'id': channel['id'],
            'title': snippet['title'],
            'description': snippet['description'],
            'video_count': statistics['videoCount'],
            'view_count': statistics['viewCount'],
            'subscriber_count': statistics['subscriberCount']
        }

    def get_my_video_ids(self):
        search_list_request = self.__client.search().list(
            part='id',
            forMine=True,
            type='video',
            order='date',
            maxResults=50,
            fields='nextPageToken,items(id(videoId))'
        )

        video_ids = []
        while search_list_request:
            search_list_response = search_list_request.execute()

            for video in search_list_response['items']:
                video_ids.append(video['id']['videoId'])

            search_list_request = self.__client.search().list_next(
                previous_request=search_list_request,
                previous_response=search_list_response)

        return video_ids

    def get_videos(self, video_ids, index=None):
        videos = []
        DFAULT_COLUMNS = ['id', 'title', 'viewCount', 'likeCount', 'dislikeCount', 'commentCount', 'duration', 'publishedAt', 'description']
        columns = DEFAULT_COLUMNS if index is None else index
        for ids in self.__chunks(video_ids, 50):
            videos_list = self.__client.videos().list(
                id=','.join(ids),
                part='snippet,contentDetails,statistics',
                fields='items(id,snippet(title,description,publishedAt),contentDetails(duration),statistics(viewCount,likeCount,dislikeCount,favoriteCount,commentCount))'
            ).execute()

            for item in videos_list['items']:
                snippet = item['snippet']
                #details = item['contentDetails']
                duration = item['contentDetails']['duration'][2:]
                statistics = item['statistics']

                videos.append([
                    item['id'],
                    snippet['title'],
                    int(statistics['viewCount']),
                    int(statistics['likeCount']),
                    int(statistics['dislikeCount']),
                    int(statistics['commentCount']),
                    duration,
                    snippet['publishedAt'],
                    snippet['description']+"\n"
                ])

        return pd.DataFrame(data=videos, columns=index)

    def __chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i+n]

def main():
    import datetime

    # datetime to filename
    now = datetime.datetime.now()
    dpath = (Path(__file__).parent/'DataCollection'/now.strftime('%y%m')).resolve(strict=True)
    fpath = dpath/now.strftime('%d.csv')
    comppath = dpath/('comp-' + fpath.name);
    # japanese index; default lang:"en"
    index_jp=['識別番号','タイトル','視聴回数','高評価','低評価','コメント数','動画時間','投稿日','概要']
    # retrieve data from youtube data api
    if not comppath.exists():
        youtube = YoutubeDataApiClient(
            YOUTUBE_DATA_CLIENT_SECRETS_FILE, YOUTUBE_DATA_API_CLIENT_SCOPES
        )
        video_ids = youtube.get_my_video_ids()
        videos = youtube.get_videos(video_ids, index=index_jp)
        videos.to_csv(str(comppath), index=True, header=True, mode='w', encoding='utf-8')
    else:
        # no connection
        print("%s is already exists!" % comppath.name)
        videos = pd.read_csv(str(comppath), index_col=0, encoding='utf-8')

    # create slim-file: eliminate video's description
    videos_slim = videos.iloc[:, 0:6]
    videos_slim.to_csv(str(fpath), index=False, mode='w', encoding='utf-8')

    # delete previous 'COMPLETE' data
    yesterday = now - datetime.timedelta(days = 1)
    predpath = (Path(__file__).parent/'DataCollection'/yesterday.strftime('%y%m')).resolve(strict=True)
    prefpath = predpath/yesterday.strftime('comp-%d.csv')
    # delete file
    if prefpath.exists(): prefpath.unlink()

if __name__ == '__main__':
    main()
