BASE_URL = "http://instagram.com"
INPUT_FILEPATH = "ig_handles.csv"
DATABASE_URI = "bolt://localhost:7687"
DATABASE_USERNAME = "neo4j"
DATABASE_PASSWORD= "ig_data"

def get_session():
    import requests
    session = requests.session()
    session.get(BASE_URL)
    return session