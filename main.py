from neo4j import GraphDatabase
import pandas as pd

class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response

class populateDatabase:

    def createPlayers(conn):
        #create player nodes
        query_string = '''
        LOAD CSV WITH HEADERS FROM 'file:///Player_Attributes.csv' AS row
        MERGE (player:Player {playerID: row.ID})
        ON CREATE SET player.firstName = row.FIRST_NAME, player.lastName = row.LAST_NAME,
        player.birthDate = row.BIRTHDATE, player.school = row.SCHOOL, player.country = row.COUNTRY,
        player.height = row.HEIGHT, player.weight = row.WEIGHT, player.seasonsPlayed = row.SEASON_EXP,
        player.jerseyNumber = row.JERSEY, player.position = row.POSITION, 
        player.roundDrafted = row.DRAFT_ROUND, player.pickDrafted = row.DRAFT_NUMBER, 
        player.pointsPerGame = row.PTS, player.assistsPerGame = row.AST, 
        player.reboundsPerGame = row.REB
        '''
        conn.query(query_string, db='neo4j')

        #set player names
        query_string = '''
        match(p:Player) set p.name = p.firstName + ' ' + p.lastName
        '''
        conn.query(query_string, db='neo4j')

    def createTeams(conn):
        #create team nodes
        query_string = '''
        LOAD CSV WITH HEADERS FROM 'file:///Team_History.csv' AS row
        MERGE (team:Team {nickname: row.NICKNAME, city: row.CITY, yearFounded: row.YEARFOUNDED})
        ON CREATE SET team.teamID = row.ID, team.yearFounded = row.YEARFOUNDED,
        team.yearActiveTill = row.YEARACTIVETILL
        '''
        conn.query(query_string, db='neo4j')

        #set team names
        query_string = '''
        match(t:Team) set t.name = t.city + ' ' + t.nickname
        '''
        conn.query(query_string, db='basketball')

        #add team attributes
        query_string = '''
        LOAD CSV WITH HEADERS FROM 'file:///Team_Attributes.csv' AS row
        MATCH (team:Team) WHERE team.teamID = row.ID AND team.yearActiveTill = '2019' SET team.abbreviation = row.ABBREVIATION,
        team.arena = row.ARENA, team.arenaCapacity = row.ARENACAPACITY, team.owner = row.OWNER, team.gm = row.GENERALMANAGER,
        team.coach = row.HEADCOACH, team.dLeague = row.DLEAGUEAFFILIATION
        '''
        conn.query(query_string, db='neo4j')

    def createRelationships(conn):
        #create played for relationships
        query_string = '''
        LOAD CSV WITH HEADERS FROM 'file:///Player_Bios.csv' AS row
        MATCH (p:Player), (t:Team) WHERE p.name = row.namePlayerBREF and t.name = row.nameTeam
        create (p)-[r:Played_For {year: row.slugSeason}]->(t)
        '''
        conn.query(query_string, db='neo4j')

        #create used to be relationships
        query_string = '''MATCH (a:Team), (b:Team) WHERE a.teamID = b.teamID AND a.yearActiveTill = '2019' AND a.name <> b.name
        CREATE (a)-[r:Used_To_Be]->(b)
        '''
        conn.query(query_string, db='neo4j')


    def loadDatabase(conn):
        conn.query("CREATE OR REPLACE DATABASE basketball")
        populateDatabase.createPlayers(conn)
        populateDatabase.createTeams(conn)
        populateDatabase.createRelationships(conn)



conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="retrieval")

# Uncomment the line below to set up the database
# populateDatabase.loadDatabase(conn)

while True:
    queryFor = input("""Would you like to query for a player or a team? (enter "exit" to exit) """)

    if(queryFor == "exit" or queryFor == "Exit" or queryFor == "EXIT" or queryFor == "e" or queryFor == "e"):
        break

    if(queryFor == "Player" or queryFor == "player" or queryFor == "PLAYER" or queryFor == "p"):
        playerName = input("Which player would you like to know more about? ")

#-------------------------Query for general player information and return results-----------------------#
        print("Player Information:\n")
        query_string = """MATCH (p:Player) WHERE p.name = '""" + playerName + """'RETURN p.name,
        p.birthdate, p.country, p.height, p.weight, p.jerseyNumber, p.roundDrafted, p.pickDrafted,
        p.position, p.seasonsPlayed, p.totalPoints, p.totalRebounds, p.totalAssists"""
        result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
        result.columns = ['Name', 'Birthday', 'Country', 'Height (cm)', 'Weight (lbs)', 'Jersey Number', 
        'Round Drafted', 'Pick Drafted', 'Position', 'Seasons', 'PPG', 
        'RPG', 'APG'
        ]
        print(result.to_string(index=False))
        print("\n")

#-------------------------Query for the given player's team history and return results-----------------------#
        print(playerName + "'s Team History:\n")
        query_string = """
        MATCH (p:Player)-[r:Played_For]->(t:Team)
        WHERE p.name = '""" + playerName + """'
        RETURN t.name, r.year
        ORDER BY r.year
        """
        result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
        result.columns = ['Team Name', 'Year ' + playerName + ' Played for Them']
        print(result.to_string(index=False))
        print("\n")

#-------------------------Query for information about the given player's teams and return results-----------------------#
        print(playerName + "'s Teams Information:\n")
        query_string = """
        MATCH (p:Player)-[r:Played_For]->(t:Team)
        WHERE p.name = '""" + playerName + """'
        RETURN DISTINCT t.name, t.city, t.coach, t.gm, t.owner, t.arena, t.arenaCapacity,
        t.yearFounded, t.yearActiveTill
        """
        result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
        result.columns = ['Team Name', 'City', 'Coach', 'General Manager', 'Owner', 'Arena', 'Arena Capacity', 
        'Year Founded', 'Year Active Until'
        ]
        print(result.to_string(index=False))
        print("\n")

    elif(queryFor == "Team" or queryFor == "team" or queryFor == "TEAM" or queryFor == "t"):
        teamName = input("Which team would you like to know more about? ")

#-------------------------Query for general team information and return results-----------------------#
        print(teamName + " Information:\n")
        query_string = """MATCH (t:Team) WHERE t.name = '""" + teamName + """'RETURN t.name, t.city, t.coach, t.gm, t.owner, t.arena, t.arenaCapacity,
        t.yearFounded, t.yearActiveTill
        """
        result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
        result.columns = ['Team Name', 'City', 'Coach', 'General Manager', 'Owner', 'Arena', 'Arena Capacity', 
        'Year Founded', 'Year Active Until'
        ]
        print(result.to_string(index=False))
        print("\n")

#-------------------------Query for information about the team's franchise history and return results-----------------------#
        print(teamName + " History:\n")
        query_string = """
        MATCH (a:Team)-[r:Used_To_Be]->(b:Team)
        WHERE a.name = '""" + teamName + """'
        RETURN b.name, b.city, b.yearFounded, b.yearActiveTill
        ORDER BY b.yearFounded
        """
        result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
        result.columns = ['Team Name', 'City', 'Year Founded', 'Year Active Until'
        ]
        print(result.to_string(index=False))
        print("\n")

#-------------------------Query for information about recent players for given team and return results-----------------------#
        print("Recent" + teamName + "Players:\n")
        query_string = """
        MATCH (p:Player)-[r:Played_For]->(t:Team)
        WHERE t.name = '""" + teamName + """'
        RETURN p.name,
        p.birthdate, p.country, p.height, p.weight, p.jerseyNumber, p.roundDrafted, p.pickDrafted,
        p.position, p.seasonsPlayed, p.totalPoints, p.totalRebounds, p.totalAssists
        ORDER BY r.year LIMIT 10
        """
        result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
        result.columns = ['Name', 'Birthday', 'Country', 'Height (cm)', 'Weight (lbs)', 'Jersey Number', 
        'Round Drafted', 'Pick Drafted', 'Position', 'Seasons', 'PPG', 
        'RPG', 'APG'
        ]
        print(result.to_string(index=False))
        print("\n")

    else:
        print("Invalid query: must be a team or player\n")