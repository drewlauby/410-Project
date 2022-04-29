from neo4j import GraphDatabase
import pandas as pd

#class to connect to local Neo4j database
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

#class to populate the database (only needed once)
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
        conn.query(query_string, db='neo4j')

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
        conn.query("CREATE OR REPLACE DATABASE neo4j")
        populateDatabase.createPlayers(conn)
        populateDatabase.createTeams(conn)
        populateDatabase.createRelationships(conn)

conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="retrieval")

# Uncomment the line below to set up the database
# populateDatabase.loadDatabase(conn)

#get names of all players and teams for user queries
query_string = """MATCH (p:Player) RETURN p.name"""
playerNames = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))

query_string = """MATCH (t:Team) RETURN t.name"""
teamNames = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))

while True:
    query = input("""What would you like to learn about basketball? (enter "exit" to exit) """)

    if(query == "exit" or query == "Exit" or query == "EXIT" or query == "E" or query == "e"):
        break

    queryFor = ' '.join(elem.capitalize() for elem in query.split())

    if("who" and "points" in queryFor.lower()):
        try:
            query_string = """MATCH (p:Player) WHERE toFloat(p.totalPoints) > 0 
            RETURN p.name, p.position, p.totalPoints ORDER BY toFloat(p.totalPoints) DESC LIMIT 10"""
            playerResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            playerResult.columns = ['Name', 'Position', 'Points Per Game']
            print(playerResult.to_string(index=False))
            print("\n")
        
        except Exception as e:
            print("Something went wrong with that query :(\n")
    
    elif("who" and "assists" in queryFor.lower()):
        try:
            query_string = """MATCH (p:Player) WHERE toFloat(p.totalAssists) > 0 
            RETURN p.name, p.position, p.totalAssists ORDER BY toFloat(p.totalAssists) DESC LIMIT 10"""
            playerResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            playerResult.columns = ['Name', 'Position', 'Assists Per Game']
            print(playerResult.to_string(index=False))
            print("\n")
        
        except Exception as e:
            print("Something went wrong with that query :(\n")

    elif("who" and "rebounds" in queryFor.lower()):
        try:
            query_string = """MATCH (p:Player) WHERE toFloat(p.totalRebounds) > 0 
            RETURN p.name, p.position, p.totalRebounds ORDER BY toFloat(p.totalRebounds) DESC LIMIT 10"""
            playerResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            playerResult.columns = ['Name', 'Position', 'Rebounds Per Game']
            print(playerResult.to_string(index=False))
            print("\n")
        
        except Exception as e:
            print("Something went wrong with that query :(\n")

    elif("who" and "tallest" in queryFor.lower()):
        try:
            query_string = """MATCH (p:Player) WHERE toFloat(p.height) > 0
            RETURN p.name, p.position, p.height ORDER BY toFloat(p.height) DESC LIMIT 1"""
            playerResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            playerResult.columns = ['Name', 'Position', 'Height (cm)']
            print(playerResult.to_string(index=False))
            print("\n")
        
        except Exception as e:
            print("Something went wrong with that query :(\n")

    elif("who" and "shortest" in queryFor.lower()):
        try:
            query_string = """MATCH (p:Player) WHERE toFloat(p.height) > 0
            RETURN p.name, p.position, p.height ORDER BY toFloat(p.height) LIMIT 1"""
            playerResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            playerResult.columns = ['Name', 'Position', 'Height (cm)']
            print(playerResult.to_string(index=False))
            print("\n")
        
        except Exception as e:
            print("Something went wrong with that query :(\n")

    elif(queryFor in playerNames.values):

        try:
#-----------------------------Query for general player information and return results--------------------------------#
            print("Player Information:\n")
            query_string = """MATCH (p:Player) WHERE p.name = '""" + queryFor + """'RETURN p.name,
            p.country, p.height, p.weight, p.jerseyNumber, p.roundDrafted, p.pickDrafted,
            p.position, p.seasonsPlayed, p.totalPoints, p.totalRebounds, p.totalAssists"""
            playerResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            playerResult.columns = ['Name', 'Country', 'Height (cm)', 'Weight (lbs)', 'Jersey Number', 
            'Round Drafted', 'Pick Drafted', 'Position', 'Seasons', 'PPG', 
            'RPG', 'APG'
            ]
            print(playerResult.to_string(index=False))
            print("\n")

    #----------------------------Query for the given player's team history and return results-----------------------#
            print(queryFor + "'s Team History:\n")
            query_string = """
            MATCH (p:Player)-[r:Played_For]->(t:Team)
            WHERE p.name = '""" + queryFor + """'
            RETURN t.name, r.year
            ORDER BY r.year
            """
            try:
                teamResult = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
                teamResult.columns = ['Team Name', 'Year ' + queryFor + ' Played for Them']
                print(teamResult.to_string(index=False))
            except Exception as e:
                print("No team information available for " + queryFor)
            print("\n")

    #-------------------------Query for information about the given player's teams and return results-----------------------#
            print(queryFor + "'s Teams Information:\n")
            query_string = """
            MATCH (p:Player)-[r:Played_For]->(t:Team)
            WHERE p.name = '""" + queryFor + """'
            RETURN DISTINCT t.name, t.city, t.coach, t.gm, t.owner, t.arena, t.arenaCapacity,
            t.yearFounded, t.yearActiveTill
            """
            try:
                result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
                result.columns = ['Team Name', 'City', 'Coach', 'General Manager', 'Owner', 'Arena', 'Arena Capacity', 
                'Year Founded', 'Year Active Until'
                ]
                print(result.to_string(index=False))
            except Exception as e:
                print("No team information available for " + queryFor)

            print("\n")

#----------------------------------Query for similar players and return results--------------------------------------#
            print("Similar Players:\n")
            try:
                if playerResult.loc[0, "Round Drafted"] == "Undrafted":
                    query_string = """
                    MATCH (p:Player) WHERE (abs(toInteger(""" + playerResult.loc[0, "PPG"] + """) - toInteger(p.totalPoints)) < 8)
                    AND p.position = '""" + playerResult.loc[0, "Position"] + """' AND p.roundDrafted = 'Undrafted'
                    AND (abs(toInteger(""" + playerResult.loc[0, "APG"] + """) - toInteger(p.totalAssists)) < 4)
                    AND (abs(toInteger(""" + playerResult.loc[0, "RPG"] + """) - toInteger(p.totalRebounds)) < 4)
                    AND p.name <> '""" + playerResult.loc[0, "Name"] + """'
                    RETURN p.name LIMIT 6
                    """
                else:
                    query_string = """
                    MATCH (p:Player) WHERE (abs(toInteger(""" + playerResult.loc[0, "PPG"] + """) - toInteger(p.totalPoints)) < 8)
                    AND p.position = '""" + playerResult.loc[0, "Position"] + """' AND toInteger(p.roundDrafted) = toInteger(""" + playerResult.loc[0, "Round Drafted"] + """)
                    AND (abs(toInteger(""" + playerResult.loc[0, "APG"] + """) - toInteger(p.totalAssists)) < 4)
                    AND (abs(toInteger(""" + playerResult.loc[0, "RPG"] + """) - toInteger(p.totalRebounds)) < 4)
                    AND p.name <> '""" + playerResult.loc[0, "Name"] + """'
                    RETURN p.name LIMIT 6
                    """
                result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
                result.columns = ['Name']
                print(result.to_string(index=False))
            except Exception as e:
                print("No similar players available\n")

        except Exception as e:
            print("Invalid player (make sure you've spelled and capitalized the name correctly)\n")

    elif(queryFor in teamNames.values):
        # teamName = input("Which team would you like to know more about? ")

        try:
#----------------------------------Query for general team information and return results-----------------------------------#
            print(queryFor + " Information:\n")
            query_string = """MATCH (t:Team) WHERE t.name = '""" + queryFor + """'RETURN t.name, t.city, t.coach, t.gm, t.owner, t.arena, t.arenaCapacity,
            t.yearFounded, t.yearActiveTill
            """
            result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            result.columns = ['Team Name', 'City', 'Coach', 'General Manager', 'Owner', 'Arena', 'Arena Capacity', 
            'Year Founded', 'Year Active Until'
            ]
            print(result.to_string(index=False))
            print("\n")

#-------------------------Query for information about the team's franchise history and return results-----------------------#
            print(queryFor + " History:\n")
            query_string = """
            MATCH (a:Team)-[r:Used_To_Be]->(b:Team)
            WHERE a.name = '""" + queryFor + """'
            RETURN b.name, b.city, b.yearFounded, b.yearActiveTill
            ORDER BY b.yearFounded
            """
            result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            result.columns = ['Team Name', 'City', 'Year Founded', 'Year Active Until']
            print(result.to_string(index=False))
            print("\n")

#-------------------------Query for information about recent players for given team and return results-----------------------#
            print("Recent " + queryFor + " Players: \n")
            query_string = """
            MATCH (p:Player)-[r:Played_For]->(t:Team)
            WHERE t.name = '""" + queryFor + """'
            RETURN p.name,
            p.country, p.height, p.weight, p.jerseyNumber, p.roundDrafted, p.pickDrafted,
            p.position, p.seasonsPlayed, p.totalPoints, p.totalRebounds, p.totalAssists
            ORDER BY r.year DESC LIMIT 10
            """
            result = pd.DataFrame(dict(_) for _ in conn.query(query_string, db='neo4j'))
            result.columns = ['Name', 'Country', 'Height (cm)', 'Weight (lbs)', 'Jersey Number', 
            'Round Drafted', 'Pick Drafted', 'Position', 'Seasons', 'PPG', 
            'RPG', 'APG'
            ]
            print(result.to_string(index=False))
            print("\n")

        except Exception as e:
            print("Invalid team (make sure you give the team and mascot, and they're spelled and capitalized correctly)\n")

    else:
        print("""Invalid query\n""")

conn.close()