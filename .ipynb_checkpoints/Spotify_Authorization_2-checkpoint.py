import spotipy
import urllib.request
from spotipy.oauth2 import SpotifyClientCredentials

client_credentials_manager = SpotifyClientCredentials(client_id='***REMOVED***', client_secret='***REMOVED***')
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)


# To authenticate with an account, we need to prompt a user to sign in. This is done using the “prompt_for_user_token” method in the “spotipy.utils” section of the package. As we do not use this for this project, this won’t be explored, but more can be read about this in the documentation for the Spotipy package [3].
# https://towardsdatascience.com/extracting-song-data-from-the-spotify-api-using-python-b1e79388d50

artist_name='The Menzingers'
search_result = sp.search(q='artist' + artist_name, type='artist')

items = search_result['artists']['items']
if len(items) > 0:
    artist = items[0]
    print(artist['name'], artist['images'][0]['url'])
    print('C:/Users/chris/OneDrive/Documents/Concert Database Project/SpotifyProject/spotify_artist_images/' + artist_name + '.jpg')
    #Saving image to the folder
    urllib.request.urlretrieve(artist['images'][0]['url'], 'C:/Users/chris/OneDrive/Documents/Concert Database Project/SpotifyProject/spotify_artist_images/' + artist_name + '.jpg')
