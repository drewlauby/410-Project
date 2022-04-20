from neo4j import GraphDatabase

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

conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="retrieval")

# conn.query("CREATE OR REPLACE DATABASE basketball")

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

#create played for relationships
query_string = '''
LOAD CSV WITH HEADERS FROM 'file:///Player_Bios.csv' AS row
MATCH (p:Player), (t:Team) WHERE p.name = row.namePlayerBREF and t.name = row.nameTeam
create (p)-[r:Played_For {year: row.slugSeason}]->(t)
'''

result = conn.query(query_string, db='neo4j')

print(result)



