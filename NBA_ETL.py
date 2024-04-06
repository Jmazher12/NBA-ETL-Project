################ Step 1: Designing an NBA sql database

import mysql.connector
import pandas as pd

# Here is the connection info to connect to my local mysql server for mysql workbench
conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="HBCfatboy09#",
  database="NBA"
)
cursor = conn.cursor()

# Create an NBA database
cursor.execute("CREATE DATABASE IF NOT EXISTS NBA")
cursor.execute("USE NBA")

# I will create five tables: 'Teams', 'Players', 'Positions', 'Games', and 'PlayerStats'
cursor.execute("""
CREATE TABLE IF NOT EXISTS Teams (
    TeamID INT AUTO_INCREMENT PRIMARY KEY,
    TeamName VARCHAR(255) NOT NULL,
    TeamCity VARCHAR(255) NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Players (
    PlayerID INT AUTO_INCREMENT PRIMARY KEY,
    PlayerName VARCHAR(255) NOT NULL,
    TeamID INT,
    FOREIGN KEY (TeamID) REFERENCES Teams(TeamID)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Positions (
    PositionID INT AUTO_INCREMENT PRIMARY KEY,
    PositionName VARCHAR(50) NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Games (
    GameID INT AUTO_INCREMENT PRIMARY KEY,
    GameDate DATE NOT NULL,
    HomeTeamID INT,
    AwayTeamID INT,
    FOREIGN KEY (HomeTeamID) REFERENCES Teams(TeamID),
    FOREIGN KEY (AwayTeamID) REFERENCES Teams(TeamID)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS PlayerStats (
    StatID INT AUTO_INCREMENT PRIMARY KEY,
    PlayerID INT,
    GameID INT,
    Points INT,
    Rebounds INT,
    Assists INT,
    FOREIGN KEY (PlayerID) REFERENCES Players(PlayerID),
    FOREIGN KEY (GameID) REFERENCES Games(GameID)
)
""")

conn.commit()



################### Step 2: Developing the ETL pipeline


# Cleaning/Loading the data in database
players_df = pd.read_csv('/Users/joshuamazher/Downloads/archive/2022-2023 NBA Player Stats - Regular.csv', sep=';', encoding='ISO-8859-1')
teams_df = pd.read_csv('/Users/joshuamazher/Downloads/teams.csv', encoding='ISO-8859-1')
games_df = pd.read_csv('/Users/joshuamazher/Documents/nba-schedule-243.csv', encoding='ISO-8859-1')

# I need to change the 'Date' column to match the Date-type in SQL
from datetime import datetime
games_df['Date'] = pd.to_datetime(games_df['Date'], format='%a %b %d %Y')
games_df['Date'] = games_df['Date'].dt.strftime('%Y-%m-%d')

# I found that using the mapping function was the best way for me to match the foreign/primary keys from different
# tables with each other

# Here I am assigning each NBA team a specific ID# which is equivalent to their 'TeamID' values
team_mapping = {
    'Boston Celtics': 1,
    'Brooklyn Nets': 2,
    'New York Knicks': 3,
    'Philadelphia 76ers': 4,
    'Toronto Raptors': 5,
    'Golden State Warriors': 6,
    'Los Angeles Clippers': 7,
    'Los Angeles Lakers': 8,
    'Phoenix Suns': 9,
    'Sacramento Kings': 10,
    'Chicago Bulls': 11,
    'Cleveland Cavaliers': 12,
    'Detroit Pistons': 13,
    'Indiana Pacers': 14,
    'Milwaukee Bucks': 15,
    'Dallas Mavericks': 16,
    'Houston Rockets': 17,
    'Memphis Grizzlies': 18,
    'New Orleans Pelicans': 19,
    'San Antonio Spurs': 20,
    'Atlanta Hawks': 21,
    'Charlotte Hornets': 22,
    'Miami Heat': 23,
    'Orlando Magic': 24,
    'Washington Wizards': 25,
    'Denver Nuggets': 26,
    'Minnesota Timberwolves': 27,
    'Oklahoma City Thunder': 28,
    'Portland Trail Blazers': 29,
    'Utah Jazz': 30
}

games_df['Home/Neutral'] = games_df['Home/Neutral'].map(team_mapping)
games_df['Visitor/Neutral'] = games_df['Visitor/Neutral'].map(team_mapping)

# Inserting data into 'Teams'
for index, row in teams_df.iterrows():
    cursor.execute("INSERT INTO Teams (TeamName, TeamCity) VALUES (%s, %s)", (row['name'], row['prefix_1']))
# Inserting data into 'Positions'
for index, row in players_df.iterrows():
    cursor.execute("INSERT INTO Positions (PositionName) VALUES (%s)", (row['Pos'],))
# Inserting data into 'Games'
for index, row in games_df.iterrows():
    cursor.execute("INSERT INTO Games (GameDate, HomeTeamID, AwayTeamID) VALUES (%s, %s, %s)",
                   (row['Date'], row['Home/Neutral'], row['Visitor/Neutral']))

# Cleaning the players table
players_df = players_df[players_df['Tm'] != 'TOT']
players_df = players_df.drop_duplicates(subset='Player')
players_df['Tm'] = players_df['Tm'].str.lower()

# Similar to the 'Games' table, I will map the 'TeamID' values to my 'Players' table
tm_mapping = {
    'bos': 1,
    'brk': 2,
    'nyk': 3,
    'phi': 4,
    'tor': 5,
    'gsw': 6,
    'lac': 7,
    'lal': 8,
    'pho': 9,
    'sac': 10,
    'chi': 11,
    'cle': 12,
    'det': 13,
    'ind': 14,
    'mil': 15,
    'dal': 16,
    'hou': 17,
    'mem': 18,
    'nop': 19,
    'sas': 20,
    'atl': 21,
    'cho': 22,
    'mia': 23,
    'orl': 24,
    'was': 25,
    'den': 26,
    'min': 27,
    'okc': 28,
    'por': 29,
    'uta': 30
}

players_df['Tm'] = players_df['Tm'].map(tm_mapping)

# Inserting data into 'Players'
for index, row in players_df.iterrows():
    cursor.execute("INSERT INTO Players (PlayerName, TeamID) VALUES (%s, %s)",
                   (row['Player'], row['Tm']))
#
sql = "SELECT PlayerName, PlayerID FROM Players"
cursor.execute(sql)

player_id_mapping = cursor.fetchall()

sql = "SELECT GameDate, GameID FROM Games"
cursor.execute(sql)

game_id_mapping = cursor.fetchall()

# Converting the csv data to a DataFrame
player_id_df = pd.DataFrame(player_id_mapping, columns=['PlayerName', 'PlayerID'])
game_id_df = pd.DataFrame(game_id_mapping, columns=['GameDate', 'GameID'])
game_id_df['GameDate'] = pd.to_datetime(game_id_df['GameDate']).dt.strftime('%Y-%m-%d')

# merged_df refers to my '2022-2023 NBA Player Stats - Regular.csv' file
# merged_game_df refers to my 'nba-schedule-243.csv' file  **Note: I have all of my downloaded csv files in my git repo
merged_df = pd.merge(players_df, player_id_df, left_on='Player', right_on='PlayerName', how='left')
merged_game_df = pd.merge(games_df, game_id_df, left_on='Date', right_on='GameDate', how='left')
merged_game_df['GameID'] = merged_game_df['GameID'].astype(int)



# Inserting data into 'PlayerStats'

# For context, I am using the NBA Player Stats csv file to extract the player statistics
# whereas for the GameID I am using the NBA Schedule csv file
for index, row in merged_df.iterrows():
    cursor.execute("INSERT INTO PlayerStats (PlayerID, Points, Rebounds, Assists) VALUES "
                   "(%s, %s, %s, %s)",
                   (row['PlayerID'], row['PTS'], row['TRB'], row['AST']))
for index, row in merged_game_df.iterrows():
    cursor.execute("INSERT INTO PlayerStats (GameID) VALUES (%s)",
                   (row['GameID'], ))


## NOTE: I decided to drop the 'GameID' column in my PlayerStats table because the
# stats reference a players averages (average PPG, average RPG, average APG). Meanwhile, the GameID is referring to
# a specific game on a certain date, so the stats for one game don't line up with a player's season average stats
# cursor.execute("ALTER TABLE PlayerStats DROP COLUMN GameID;")  **NOTE: I dropped the column in mysql**

conn.commit()


# All of my data has successfully loaded into mysql server. I will upload screenshots of the tables with all the data
# loaded in them in my git repo


# API Requirement: (For context I am using python's integrated NBA_API library that has NBA statistics on players
import requests
import requests.exceptions
import os
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats

# Prompt the user to enter a player's name
player_name = input("Please enter the specific player's name you want to display information on: ")

# Find the player by full name
player_dict = players.find_players_by_full_name(player_name)

if player_dict:
    player_id = player_dict[0]['id']
    player_full_name = player_dict[0]['full_name']

    # Fetching the player's career statistics
    career = playercareerstats.PlayerCareerStats(player_id=player_id)
    career_df = career.get_data_frames()[0]  # Get the DataFrame from the response

    # Display basic information and statistics
    print(f"\nDisplaying information for {player_full_name}:")
    print(career_df[['SEASON_ID', 'TEAM_ID', 'GP', 'PTS', 'REB', 'AST']])
else:
    print("Player not found. Please make sure the name is spelled correctly.")


cursor.close()
conn.close()


##################### Step 3: SQL Queries

# Below are sql queries that can be run in my database
## Note: The statements are commented out just to document the statements I used in sql

# 1) What team does LaMelo Ball play for? :
# SELECT Players.PlayerName, Teams.TeamName
# FROM Players
# JOIN Teams ON Players.TeamID = Teams.TeamID
# WHERE Players.PlayerName = 'LaMelo Ball';

# 2) How many points does Stephen Curry average per game? :
# SELECT Players.PlayerName, PlayerStats.Points
# FROM Players
# JOIN PlayerStats ON Players.PlayerID = PlayerStats.PlayerID
# WHERE Players.PlayerName = 'Stephen Curry';

# 3) Give me a list of teams that played on 2023-10-25 :
# SELECT GameDate, HomeTeam.TeamName AS HomeTeam, AwayTeam.TeamName AS AwayTeam
# FROM Games
# JOIN Teams AS HomeTeam ON Games.HomeTeamID = HomeTeam.TeamID
# JOIN Teams AS AwayTeam ON Games.AwayTeamID = AwayTeam.TeamID
# WHERE GameDate = '2023-10-25';

# 4) What is the most popular position?
# SELECT PositionName, COUNT(*) AS PositionCount
# FROM Positions
# GROUP BY PositionName
# ORDER BY PositionCount DESC
# LIMIT 1;

# 5) Who scores the most points per game?
# SELECT Players.PlayerID, Players.PlayerName, PlayerStats.Points AS HighestPoints
# FROM Players
# JOIN PlayerStats ON Players.PlayerID = PlayerStats.PlayerID
# WHERE PlayerStats.Points = (
#     SELECT MAX(Points)
#     FROM PlayerStats
# )
# ORDER BY HighestPoints DESC
# LIMIT 1;