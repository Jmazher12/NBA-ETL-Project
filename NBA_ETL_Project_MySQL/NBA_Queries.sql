SELECT Players.PlayerName, Teams.TeamName
FROM Players
JOIN Teams ON Players.TeamID = Teams.TeamID
WHERE Players.PlayerName = 'LaMelo Ball';

SELECT Players.PlayerName, PlayerStats.Points
FROM Players
JOIN PlayerStats ON Players.PlayerID = PlayerStats.PlayerID
WHERE Players.PlayerName = 'Stephen Curry';

SELECT GameDate, HomeTeam.TeamName AS HomeTeam, AwayTeam.TeamName AS AwayTeam
FROM Games
JOIN Teams AS HomeTeam ON Games.HomeTeamID = HomeTeam.TeamID
JOIN Teams AS AwayTeam ON Games.AwayTeamID = AwayTeam.TeamID
WHERE GameDate = '2023-10-25';

SELECT PositionName, COUNT(*) AS PositionCount
FROM Positions
GROUP BY PositionName
ORDER BY PositionCount DESC
LIMIT 1;

SELECT Players.PlayerID, Players.PlayerName, PlayerStats.Points AS HighestPoints
FROM Players
JOIN PlayerStats ON Players.PlayerID = PlayerStats.PlayerID
WHERE PlayerStats.Points = (
    SELECT MAX(Points)
    FROM PlayerStats
)
ORDER BY HighestPoints DESC
LIMIT 1;



