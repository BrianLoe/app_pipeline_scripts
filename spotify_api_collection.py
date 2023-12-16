import pandas as pd
import os
import json
import requests
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import base64

## artist_details
# records_dict = {
#     'artist_id':[],
#     'artist_followers':[],
#     'artist_popularity':[],
#     'artist_image_url':[],
#     'artist_link':[]
# }

## audio_features
# result_dict = {
#     'track_id': [],
#     'acousticness' : [],
#     'danceability' : [],
#     'duration_ms' : [],
#     'energy' : [],
#     'instrumentalness' : [],
#     'key':[],
#     'liveness' : [],
#     'loudness' : [],
#     'mode' : [],
#     'speechiness' : [],
#     'tempo' : [],
#     'time_signature' : [],
#     'valence':[]
# }

class SpotipyScraper:
    def __init__(self, method='api', client_id=None, client_secret=None):
        self.client = None
        if method=='api':
            self.client = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
        self.access_token = None
        self.result_dict = None
    
    def _get_access_token(self):
        access_token_url = "https://accounts.spotify.com/api/token"
        response = requests.post(
            access_token_url,
            headers={
                "Authorization": "Basic "
                + base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "client_credentials",
            },
        )
        if response.status_code != 200:
            print(response.status_code)
            raise Exception
        access_token = response.json()["access_token"]
        self.access_token = access_token 
        
    def get_artist_details(self, artist_ids):
        self.result_dict = {
            'artist_id':[],
            'artist_name':[],
            'artist_popularity':[],
            'artist_image_url':[],
            'artist_followers':[],
            'artist_url':[]
        }
        if self.client:
            artist_response = self.client.artists(artist_ids)['artists']
            for artist in artist_response:
                self.result_dict['artist_id'].append(artist['id'])
                self.result_dict['artist_name'].append(artist['name'])
                self.result_dict['artist_followers'].append(artist['followers']['total'])
                idx = int(len(artist['images'])/2)
                self.result_dict['artist_image_url'].append(artist['images'][idx]['url'])
                self.result_dict['artist_url'].append(artist['external_urls']['spotify'])
                self.result_dict['artist_popularity'].append(artist['popularity'])
                
    def get_audio_features(self, track_ids):
        self.result_dict = {
            'track_id': [],
            'acousticness' : [],
            'danceability' : [],
            'duration_ms' : [],
            'energy' : [],
            'instrumentalness' : [],
            'key':[],
            'liveness' : [],
            'loudness' : [],
            'mode' : [],
            'speechiness' : [],
            'tempo' : [],
            'time_signature' : [],
            'valence':[]
            }
        if self.client:
            features_response = self.client.audio_features(track_ids)
            for feature in features_response:
                self.result_dict['track_id'].append(feature['id'])
                self.result_dict['acousticness'].append(feature['acousticness'])
                self.result_dict['danceability'].append(feature['danceability'])
                self.result_dict['duration_ms'].append(feature['duration_ms'])
                self.result_dict['energy'].append(feature['energy'])
                self.result_dict['instrumentalness'].append(feature['instrumentalness'])
                self.result_dict['key'].append(feature['key'])
                self.result_dict['liveness'].append(feature['liveness'])
                self.result_dict['loudness'].append(feature['loudness'])
                self.result_dict['mode'].append(feature['mode'])
                self.result_dict['speechiness'].append(feature['speechiness'])
                self.result_dict['tempo'].append(feature['tempo'])
                self.result_dict['time_signature'].append(feature['time_signature'])
                self.result_dict['valence'].append(feature['valence'])
                
    def get_playlist(self, playlist_id):
        self.result_dict = {
            
        }
        
        if self.client:
            playlist_response = self.client.playlist(playlist_id)['tracks']
            
                
    def get_track_details(self, track_ids):
        self.result_dict = {
            'track_id':[],
            'track_name':[],
            'track_url':[],
            'track_preview_url':[],
            'track_popularity':[],
            'track_image_url':[],
            'track_release_year':[],
            'artist_id':[]
        }
        if self.client:
            track_response = self.client.tracks(track_ids)['tracks']
            for track in track_response:
                self.result_dict['track_id'].append(track['id'])
                self.result_dict['track_name'].append(track['name'])
                self.result_dict['track_url'].append(track['external_urls']['spotify'])
                self.result_dict['track_preview_url'].append(track['preview_url'])
                self.result_dict['track_popularity'].append(track['popularity'])
                idx = int(len(track['album']['images'])/2)
                self.result_dict['track_image_url'].append(track['album']['images'][idx]['url'])
                precision = track['album']['release_date_precision']
                if precision == 'year':
                    self.result_dict['track_release_year'].append(track['album']['release_date'])
                else:
                    self.result_dict['track_release_year'].append(track['album']['release_date'].split('-')[0])
                self.result_dict['artist_id'].append(track['artists'][0]['id'])
        
    def save_to_csv(self, fname="data.csv"):
        if not self.result_dict:
            print('No data was scraped')
            raise Exception
        if any(self.result_dict.values()):
            print("Saving data to CSV...")
            df = pd.DataFrame(self.result_dict)
            df.to_csv(fname, index=False)
            
    def post_to_oracledb(self, endpoint):
        if not self.result_dict:
            print('No data was scraped')
            raise Exception
        if any(self.result_dict.values()):
            df = pd.DataFrame(self.result_dict)
            records = df.to_dict('records')
            records_json = json.dumps({'payload':records})
            url = f"https://g9c989a618c1148-spotifydb.adb.ap-sydney-1.oraclecloudapps.com/ords/admin/api/{endpoint}/"
            response = requests.post(url, data=records_json)
            if response.status_code != 200:
                print(response.text)
                raise Exception

load_dotenv()
# client_secret, client_id = os.getenv("SPOTIPY_CLIENT_SECRET"), os.getenv("SPOTIPY_CLIENT_ID")
if __name__ == '__main__':
    data = pd.read_csv('top_50_global.csv')
    track_ids = data['track_id'].tolist()
    collecter = SpotipyScraper()
    collecter.get_track_details(track_ids)
    collecter.save_to_csv('track_details.csv')
    collecter.post_to_oracledb('tracks')







# for i in range(0, len(track_data), batches):
#     tracks = ",".join(track_data.iloc[i : i + batches]["track_id"].values.tolist())
#     max_retries = 3
#     retries = 0
#     while retries < max_retries:
#         response = requests.get(
#             f"https://api.spotify.com/v1/tracks?ids={tracks}", headers=headers
#         )
#         if response.status_code == 401:
#             print("Token Expired, Getting New Token...")
#             token_retries = 0
#             while token_retries < max_retries:
#                 try:
#                     access_token = get_access_token(client_secret, client_id)
#                 except Exception as e:
#                     print(f"Error caught: {e} | Retrying ...")
#                     token_retries += 1
#                     time.sleep(5)
#                     continue
#             if token_retries >= max_retries:
#                 print("Max token retries reached, exiting...")
#                 raise SystemExit(1)
#             headers = {"Authorization": "Bearer " + access_token}
#             retries += 1
#             continue
#         elif response.status_code == 429:
#             print("Rate limit reached, sleeping for 60 seconds...")
#             retries += 1
#             time.sleep(60)
#             continue
#         elif response.status_code != 200:
#             print(response.status_code)
#             save_to_csv(records_dict, 'partial_artists_spotify.csv')
#             raise SystemExit(1)
        
#     if retries >= max_retries:
#         print("Max retries reached, exiting...")
#         save_to_csv(records_dict, 'partial_artists_spotify.csv')
#         raise SystemExit(1)
    
#     try:
#         tracks_data = response.json()["tracks"]
#     except Exception as e:
#         print(f"Error: {e}")
#         save_to_csv(records_dict, 'partial_artists_spotify.csv')
#         continue
    
#     for track in tracks_data:
#         if track:
#             artist_dict = track["artists"][0]
#             records_dict["artist_id"].append(artist_dict["id"])
#             records_dict["artist_names"].append(artist_dict["name"])
#         else:
#             print("Track data is nonetype")
#             continue
#     time.sleep(5)
# save_to_csv(records_dict, 'artists_spotify.csv')