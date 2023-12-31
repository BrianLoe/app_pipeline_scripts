import pandas as pd
import numpy as np
import os
import json
import requests
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import base64

class SpotipyScraper:
    def __init__(self, method='api', 
                 spotify_client_id=None, spotify_client_secret=None, 
                 oracle_client_id=None, oracle_client_secret=None):
        self.client = None
        if method=='api':
            self.client = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
            
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.oracle_client_id = oracle_client_id
        self.oracle_client_secret = oracle_client_secret
        
        self.spotify_access_token = None
        self.oracle_access_token = None
        self.result_dict = None
        
        response = requests.get("https://g9c989a618c1148-spotifydb.adb.ap-sydney-1.oraclecloudapps.com/ords/admin/api/get-trendings").json()
        self.current_trending_tracks = pd.DataFrame(response['items'])
        self.artist_ids = []
    
    def _get_spotify_access_token(self):
        access_token_url = "https://accounts.spotify.com/api/token"
        response = requests.post(
            access_token_url,
            headers={
                "Authorization": "Basic "
                + base64.b64encode(f"{self.spotify_client_id}:{self.spotify_client_secret}".encode()).decode(),
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
        
    def _get_oracle_access_token(self):
        identity_url = "https://g9c989a618c1148-spotifydb.adb.ap-sydney-1.oraclecloudapps.com/ords/admin/oauth/token"
        response = requests.post(identity_url, 
                                 data={'grant_type':'client_credentials'}, 
                                 headers={'Authorization':'Basic '
                                          +base64.b64encode(f"{self.oracle_client_id}:{self.oracle_client_secret}".encode()).decode()}
                                 )
        if response.status_code != 200:
            print(response.status_code)
            raise Exception
        self.oracle_access_token = response.json()['access_token']
        
    def get_artist_details(self, artist_ids):
        batch = 50
        if self.client:
            for i in range(0, len(artist_ids), batch):
                self.result_dict = {
                    'artist_id':[],
                    'artist_name':[],
                    'artist_popularity':[],
                    'artist_image_url':[],
                    'artist_followers':[],
                    'artist_url':[]
                }
                artist_response = self.client.artists(artist_ids[i:i+batch])['artists']
                for artist in artist_response:
                    self.result_dict['artist_id'].append(artist['id'])
                    self.result_dict['artist_name'].append(artist['name'])
                    self.result_dict['artist_followers'].append(artist['followers']['total'])
                    if len(artist['images']) > 0:
                        idx = int(len(artist['images'])/2)
                        self.result_dict['artist_image_url'].append(artist['images'][idx]['url'])
                    else:
                        self.result_dict['artist_image_url'].append(None)
                    self.result_dict['artist_url'].append(artist['external_urls']['spotify'])
                    self.result_dict['artist_popularity'].append(artist['popularity'])
                flag = self._check_empty_result_dict()
                if not flag:
                    self.post_to_oracledb('artists')
                time.sleep(3)
                
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
                
    def get_playlist(self, playlist_id, update_trending=False, store_artist_ids=False):
        self.result_dict = {
            'track_id':[],
            'track_name':[],
            'track_url':[],
            'track_preview_url':[],
            'track_popularity':[],
            'track_image_url':[],
            'track_release_year':[],
            'artist_id':[],
            'track_rank':[],
        }
        if self.client:
            playlist_response = self.client.playlist(playlist_id)['tracks']['items'] 
            for rank, track in enumerate(playlist_response):
                the_track = track['track']
                self.result_dict['track_preview_url'].append(the_track['preview_url'])
                self.result_dict['track_id'].append(the_track['id'])
                self.result_dict['track_name'].append(the_track['name'])
                self.result_dict['track_url'].append(the_track['external_urls']['spotify'])
                self.result_dict['track_popularity'].append(the_track['popularity'])
                if len(the_track['album']['images']) > 0:
                    idx = int(len(the_track['album']['images'])/2)
                    self.result_dict['track_image_url'].append(the_track['album']['images'][idx]['url'])
                else:
                    self.result_dict['track_image_url'].append(None)
                precision = the_track['album']['release_date_precision']
                if precision == 'year':
                    self.result_dict['track_release_year'].append(the_track['album']['release_date'])
                else:
                    self.result_dict['track_release_year'].append(the_track['album']['release_date'].split('-')[0])
                self.result_dict['artist_id'].append(the_track['artists'][0]['id'])
                self.result_dict['track_rank'].append(rank+1)
            flag = self._check_empty_result_dict()
            if not flag:
                temp_resultdict = self.result_dict.copy()
                if store_artist_ids:
                    self.artist_ids += self.result_dict['artist_id']
                del self.result_dict['track_rank']
                self.post_to_oracledb('tracks')
                if update_trending:
                    temp_dict = {key: temp_resultdict[key] for key in ['track_id', 'track_rank']}
                    if self.current_trending_tracks.empty:
                        temp_dict['status'] = ['still'] * len(temp_dict['track_id'])
                        self.result_dict = temp_dict
                    else:
                        current_set = set(self.current_trending_tracks['track_id'].tolist())
                        new_set = set(temp_resultdict['track_id'])
                        new_tracks = new_set - current_set
                        new_df = pd.DataFrame(temp_dict)
                        # Filtering existing tracks
                        temp_df = pd.merge(self.current_trending_tracks, new_df, on='track_id', how='left', suffixes=('_old','_new'))
                        temp_df['rank_diff'] = temp_df['track_rank_new'] - temp_df['track_rank_old']
                        temp_df['status'] = np.where(temp_df['rank_diff'] > 0, 'up', None)
                        temp_df['status'] = np.where(temp_df['rank_diff'] < 0, 'down', temp_df['status'])
                        temp_df['status'] = np.where(temp_df['rank_diff'] == 0, 'still', temp_df['status'])
                        temp_df = temp_df.drop(columns=['rank_diff', 'track_rank_old'])
                        temp_df = temp_df.rename(columns={'track_rank_new':'track_rank'})
                        # New tracks
                        new_df = new_df.loc[new_df['track_id'].isin(new_tracks)]
                        temp_df = pd.concat([temp_df, new_df])
                        temp_df['status'] = temp_df['status'].fillna('new')
                        self.result_dict = temp_df.to_dict('records')
                    self.post_to_oracledb('trending')
        
    def get_track_details(self, track_ids, store_artist_ids=False):
        batch = 50
        if self.client:
            for i in range(0, len(track_ids), batch):
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
                track_response = self.client.tracks(track_ids[i:i+batch])['tracks']
                for track in track_response:
                    if track['preview_url'] is None:
                        continue
                    self.result_dict['track_preview_url'].append(track['preview_url'])
                    self.result_dict['track_id'].append(track['id'])
                    self.result_dict['track_name'].append(track['name'])
                    self.result_dict['track_url'].append(track['external_urls']['spotify'])
                    self.result_dict['track_popularity'].append(track['popularity'])
                    if len(track['album']['images']) > 0:
                        idx = int(len(track['album']['images'])/2)
                        self.result_dict['track_image_url'].append(track['album']['images'][idx]['url'])
                    else:
                        self.result_dict['track_image_url'].append(None)
                    precision = track['album']['release_date_precision']
                    if precision == 'year':
                        self.result_dict['track_release_year'].append(track['album']['release_date'])
                    else:
                        self.result_dict['track_release_year'].append(track['album']['release_date'].split('-')[0])
                    self.result_dict['artist_id'].append(track['artists'][0]['id'])
                if store_artist_ids:
                    self.artist_ids += self.result_dict['artist_id']
                flag = self._check_empty_result_dict()
                if not flag:
                    self.post_to_oracledb('tracks')
                time.sleep(3)
    
    def _check_empty_result_dict(self):
        try:
            if any(self.result_dict.values()):
                return False
        except AttributeError as e:
            if len(self.result_dict) > 0:
                return False
        return True    
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
        if not self.oracle_access_token:
            self._get_oracle_access_token()
        if isinstance(self.result_dict, dict):
            df = pd.DataFrame(self.result_dict)
            records = df.to_dict('records')
        else:
            records = self.result_dict
        records_json = json.dumps({'payload':records})
        url = f"https://g9c989a618c1148-spotifydb.adb.ap-sydney-1.oraclecloudapps.com/ords/admin/upsert/{endpoint}/"
        response = requests.post(url, data=records_json, headers={'Authorization': 'Bearer ' + self.oracle_access_token})
        
        if response.status_code == 401:
            print("Token Expired, Getting New Token...")
            self.oracle_access_token = None
            self.post_to_oracledb(endpoint)
        if response.text != '':
            print(response.text)
            print('Warning, there was an error upserting into the database, PL/SQL has an error. See above.')
        if response.status_code != 200:
            print(response.text)
            raise Exception
        print(f"Successfully upserted {len(records)} records to {endpoint}")

load_dotenv()
# client_secret, client_id = os.getenv("SPOTIPY_CLIENT_SECRET"), os.getenv("SPOTIPY_CLIENT_ID")
if __name__ == '__main__':
    # data = pd.read_csv('data/artists_temp.csv')
    # artist_ids = data['artist_id'].tolist()[98:]
    collecter = SpotipyScraper(oracle_client_id=os.getenv("ORACLE_CLIENT_ID"), 
                               oracle_client_secret=os.getenv("ORACLE_CLIENT_SECRET"))  
    # p_id = "6UeSakyzhiEt4NB3UAd6NQ" # Billboard 100
    p_id = "37i9dQZEVXbMDoHDwVN2tF" # top50 global playlist id
    collecter.get_playlist(p_id, update_trending=True, store_artist_ids=True) 
    # for _ in range(10): 
    #     response = requests.get("https://g9c989a618c1148-spotifydb.adb.ap-sydney-1.oraclecloudapps.com/ords/admin/api/tracks/missing")
    #     data = response.json()['items']
    #     track_ids = [d['track_id'] for d in data]
        
    #     collecter.get_track_details(track_ids, store_artist_ids=True)
    # collecter.save_to_csv('track_details.csv')
    # collecter.post_to_oracledb('tracks')
    
    # artist_ids = collecter.artist_ids
    
    # collecter.get_artist_details(collecter.artist_ids)
    # collecter.save_to_csv('artist_details.csv')
    # collecter.post_to_oracledb('artists')



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
