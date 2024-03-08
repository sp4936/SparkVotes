import random
import urllib.parse
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer

#------------------------------------API Data Fetching-------------------------------------------------------
BASE_URL = 'https://randomuser.me/api/?nat=IN'
PARTIES = ["Bhartiya Janta Party", "Indian National Congress Party", "Aam Aadmi Party"]
random.seed(42)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

#------------------------------Create Table(Candidate,Voters,Votes)----------------------------------------
def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()



#------------------------------------Insert Candidate Data Into Database------------------------------------
def insert_candidate(conn, cur, candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url):
    encoded_url = urllib.parse.quote_plus(photo_url)
    query = """
        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(query, (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, encoded_url))
    conn.commit()
    
    

#------------------------------Generate Voters Data---------------------------------------------------------
def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"
    
    
#---------------------------------Insert Voters Data Into Database------------------------------------------
def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()






# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'


#--------------------------------------Entry Point of Application------------------------------------------
if __name__ == "__main__":                        #entry point of application
    
    #Database Connection
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    #Kafka_Producer
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    
    #Table Creation
    create_tables(conn, cur)

    # get candidates from db
    cur.execute("""
        SELECT * FROM candidates
    """)
    candidates = cur.fetchall()
    #print(candidates)
    
    # get voters from db
    cur.execute("""
                SELECT * FROM voters
                """)
    voters = cur.fetchall()

    '''if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            print(candidate)
            cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
            conn.commit() '''
            
    if len(candidates) == 0:
           
            #01
            insert_candidate(conn, cur, "1", "Narendra Modi", "Bhartiya Janta Party", "Member of RSS", "Rally,Social Media", "https://wallpapercave.com/wp/wp6727827.jpg")
            
            #02
            insert_candidate(conn, cur, "2","Rahul Gandhi","Indian National Congress Party", "Member of National Congress", "Rally,Social Media, Bharat Jodo Yatra", "https://www.dailyexcelsior.com/wp-content/uploads/2020/04/rahul.jpg")
           
            #03
            insert_candidate(conn, cur, "3","Arvind Kejrival","Aam Aadmi Party", "Member of Aam Aadmi Party", "Rally,Social Media", "https://www.letuspublish.com/wp-content/uploads/2014/08/ArvindKejriwalphoto.jpg")
            
#    if len(voters) == 0:
    for i in range(1000):
            voter_data = generate_voter_data()
            insert_voters(conn, cur, voter_data)

            producer.produce(
                voters_topic,
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )

            print('Produced voter {}, data: {}'.format(i, voter_data))
            producer.flush()                                               #to insure all data is delivered