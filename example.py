from pprint import pprint 
from DbConnector import DbConnector
from datetime import datetime

import os
from tqdm import tqdm




class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_activities(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_activities(self, collection_name):
        collection = self.db[collection_name]
        activities = collection.find({})
        for doc in activities: 
            print(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         

class Part1:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.base_path = os.path.join("dataset", "Data")

        self.trackpointsum = 0
        self.trackpointsum_nofilter = 0

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_users(self, collection_name = "User"):   

        with open(os.path.join("dataset", 'labeled_ids.txt')) as file:
            users_with_labels = file.readlines()
            users_with_labels = [int(s.rstrip()) for s in users_with_labels]

        docs = []

        for id in os.listdir(self.base_path):
            docs.append({
                "_id": id,
                "has_labels": (int(id) in users_with_labels)
            })

        collection = self.db[collection_name]
        collection.insert_many(docs)

    def insert_activitydata(self, collection_name = "Activity"):
        docs = []


        with open(os.path.join("dataset", 'labeled_ids.txt')) as file:
            users_with_labels = file.readlines()
            users_with_labels = [s.rstrip() for s in users_with_labels]
            users_without_labels = list(filter(lambda user: user not in users_with_labels, os.listdir(os.path.join("dataset", "Data"))))
        
        print("----------------------------------")
        print("Parsing ActivityData")       
        print("----------------------------------")
        for user in tqdm(users_with_labels):
            labels_path = os.path.join("dataset", "Data", user, "labels.txt")

            with open(labels_path) as labels_file:
                for line in labels_file.readlines()[1:]:
                    line_data = (line.rstrip().split('\t'))
                    

                    docs.append(
                    {
                        'user_id' : user,
                        'transportation_mode': line_data[2],
                        'start_date_time': datetime.strptime(line_data[0], "%Y/%m/%d %H:%M:%S"),
                        'end_date_time': datetime.strptime(line_data[1], "%Y/%m/%d %H:%M:%S"),
                    }
                    )

        for user in tqdm(users_without_labels):
            folder_path = os.path.join("dataset", "Data", user, "Trajectory")

            for activity_file_name in os.listdir(folder_path):
                with open(os.path.join(folder_path, activity_file_name)) as activity_file:

                    lines = activity_file.readlines()
                    start_date_time = lines[6].rstrip().split(',')[-2] + ' ' + lines[6].rstrip().split(',')[-1]
                    end_date_time = lines[-1].rstrip().split(',')[-2] + ' ' + lines[-1].rstrip().split(',')[-1]

                    docs.append(
                    {
                        'user_id' : user,
                        'transportation_mode': None,
                        'start_date_time': datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S"),
                        'end_date_time': datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S"),
                    }
                    )




        print("----------------------------------")
        print("Adding ActivityData to the Database")       
        print("----------------------------------")
        collection = self.db[collection_name]
        collection.insert_many(docs)

    def insert_trackPointdata(self, collection_name = "TrackPoint"):
        print("----------------------------------")
        print("Parsing and adding Trackpoints to the Database")       
        print("----------------------------------")
        for folder in tqdm(os.listdir(self.base_path)):

            #Step 1:
            # get_activities = "SELECT id, start_date_time, end_date_time FROM Activity where user_id = '%s' ORDER BY start_date_time, end_date_time"
            # self.cursor.execute(get_activities % (folder))
            # activities = self.cursor.fetchall()

            collection = self.db['Activity']
            activities = list(collection.find({'user_id': folder}))
            activities = sorted(activities, key = lambda obj: (obj['start_date_time'], obj['end_date_time']))
            

            if len(activities) == 0:
                continue

            #Step 2:
            
            docs = []
            
            for filename in os.listdir(os.path.join(self.base_path, folder, 'Trajectory')):
                with open(os.path.join(self.base_path, folder, 'Trajectory', filename)) as file:
                    lines = file.readlines()[6:]

                    if len(lines) <= 2500:

                        for line in lines:
                            line_data = (line.rstrip().split(','))

                            docs.append(
                            {
                                'lat': line_data[0],
                                'lon': line_data[1],
                                'altitude': line_data[3],
                                'date_days': line_data[4],
                                'date_time':  datetime.strptime(line_data[5] + ' ' + line_data[6], "%Y-%m-%d %H:%M:%S"),
                                'activity_id': None
                            }
                            )
   
            #Step 3:

            self.find_matching_activities(activities, docs)
    
            #Step 4:
            
            self.trackpointsum_nofilter += len(docs)

            docs = list(filter(lambda obj: obj['activity_id'] is not None, docs))

            self.trackpointsum += len(docs)

            if len(docs) > 0:

                collection = self.db[collection_name]
                collection.insert_many(docs)


            

    def find_matching_activities(self, activities, data):
        
        print("----------------------------------")
        print(f"Finding matching activities for {len(data)} Trackpoints and {len(activities)} activities")       
        print("----------------------------------")
        
        #Binary search for a corresponding activity:

        for i in tqdm(range(len(data))):
            
            lower_bound = 0
            upper_bound = len(activities) - 1

            success = False

            while lower_bound < upper_bound:

                j = int(lower_bound + (upper_bound - lower_bound)/2)

                #Starttime of activity after the timestamp 
                
                if activities[j]['start_date_time'] > data[i]['date_time']:
                    upper_bound = j - 1

                #Endtime of activity before the timestamp
                elif activities[j]['end_date_time'] < data[i]['date_time']:
                    lower_bound = j + 1

                #Found a corresponding activity
                else:
                    data[i]['activity_id'] = activities[j]['_id']
                    success = True
                    break

            if not success:
                data[i]['activity_id'] = None
        
    def fetch_activities(self, collection_name):
        collection = self.db[collection_name]
        activities = collection.find({})
        for doc in activities: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

        print(self.trackpointsum, self.trackpointsum_nofilter)



def main():
    program = None
    try:
        program = Part1()
        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="TrackPoint")

        program.insert_users()
        program.insert_activitydata()
        program.insert_trackPointdata()

        program.show_coll()
        # program.fetch_activities(collection_name="User")
        # program.fetch_activities(collection_name="Activity")
        # program.fetch_activities(collection_name="TrackPoint")

        #TODO: REMOVE LATER
        # program.drop_coll(collection_name='User')
        # program.drop_coll(collection_name='Activity')
        #program.drop_coll(collection_name='TrackPoint')
        
        # Check that the table is dropped
        # program.show_coll()
    except Exception as e:
        # program.drop_coll(collection_name='User')
        # program.drop_coll(collection_name='Activity')
        # program.drop_coll(collection_name='TrackPoint')

        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

def main_example():
    program = None
    try:
        program = ExampleProgram()
        program.create_coll(collection_name="Person")
        program.show_coll()
        program.insert_activities(collection_name="Person")
        program.fetch_activities(collection_name="Person")
        program.drop_coll(collection_name="Person")
        # program.drop_coll(collection_name='person')
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        program.show_coll()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
