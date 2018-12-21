from neo4j.v1 import GraphDatabase
import json,config

class DatabaseDriver:
    def __init__(self,uri,user,password):
        self._driver = GraphDatabase.driver(uri,auth=(user,password))

    def close(self):
        self._driver.close()

    #takes a full ig_obj and creates the user,posts, hashtags
    def create_user(self,user_json):
        id,bio,username,followers,full_name = user_json['id'],user_json['bio'],user_json['username'],user_json['followers'],user_json['full_name']
        posts = user_json['posts']
        with self._driver.session() as session:
            session.run("CREATE (a:Person {id: $id,bio:$bio,username:$username,followers:$followers,full_name:$full_name})",id=id,bio=bio,username=username,followers=followers,full_name=full_name)
        for post in posts:
            self.create_post(post,id)

    #takes a post and creates the post and hashtags/mentions
    def create_post(self,post_json,user_id):
        id,caption,timestamp,likes,mentions,hashtags = post_json['id'],post_json['caption'],post_json['timestamp'],post_json['likes'],post_json['mentions'],post_json['hashtags']
        with self._driver.session() as session:
            session.run("CREATE (a:Post {id:$id,caption:$caption,timestamp:$timestamp,likes:$likes})",id=id,caption=caption,timestamp=timestamp,likes=likes)
            session.run("MATCH (b:Post{id:$id}),(a:Person{id:$user_id}) CREATE (a)-[:PUBLISHED]->(b)",id=id,user_id=user_id)
        for tag in hashtags:
            self.create_hashtag(tag,id)
        for mention in mentions:
            self.create_mention(mention,id)

    def create_hashtag(self,hashtag,post_id):
        with self._driver.session() as session:
            #merging here as to not create more than one of each hashtag
            session.run("MERGE (a:Hashtag{tag:$tag})",tag=hashtag)
            session.run("MATCH (a:Post{id:$id}),(b:Hashtag{tag:$tag}) CREATE (a)-[:CONTAINS]->(b)",id=post_id,tag=hashtag)

    def create_mention(self,mention,post_id):
        with self._driver.session() as session:
            #merging here as to not create more than one of each hashtag
            session.run("MERGE (a:Mention{mention:$mention})",mention=mention)
            session.run("MATCH (a:Post{id:$id}),(b:Mention{mention:$mention}) CREATE (a)-[:CONTAINS]->(b)",id=post_id,mention=mention)