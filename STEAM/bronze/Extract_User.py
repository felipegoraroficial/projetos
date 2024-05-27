# Databricks notebook source
from steam_web_api import Steam
import json

KEY = "1A1F4922939AFD7DFB99B30C31BCE24F"
steam = Steam(KEY)

user = steam.users.get_owned_games("76561198991169245")

with open('/lakehouse/default/Files/Raw/steam_user.json', 'w') as f:
    json.dump(user, f)
