from steam_web_api import Steam
import pandas as pd
import json

KEY = "1A1F4922939AFD7DFB99B30C31BCE24F"
steam = Steam(KEY)

# arguments: search
user = steam.apps.search_games('105600')
