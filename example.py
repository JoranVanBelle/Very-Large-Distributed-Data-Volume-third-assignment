from pprint import pprint 
from DbConnector import DbConnector
from datetime import datetime

import os
from tqdm import tqdm



class Part1:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.base_path = os.path.join("dataset", "Data")

        self.trackpointsum = 0
        self.trackpointsum_nofilter = 0

    def create_coll(self, collection_name):
        """Creates a collection inside the MongoDB database.

        Args:
            collection_name (str): The name of the collection to create.
        """        

        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_users(self, collection_name = "User"):   
        """ This function inserts the user data into the user collection of our database.
            The data for the "has_labels" field is extracted from the labeld_ids.txt file.
            The user_ids themselves are just the names of the folders in the Data folder of the dataset.

        Args:
            collection_name (string): The name of the collection we want to insert the data into. Defaults to "User".
        """        

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
        """ This function inserts the activities into the activity collection of our database.
            
            Since only the folders of the users with "has_labels = 1" contain activity data, we only need to process these users.
            After parsing all the activities, we add the activites to our activity collection in the database.


        Args:
            collection_name (string): The name of the collection we want to insert the data into. Defaults to "Activity".
        """   

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
        """ This function inserts the trackpoints into the trackpoint collection of our database.
            For this the function loops through all the users, and then performs the following steps:
                1. Get all the activities corresponding to this user from the activities collection.
                   If the user has no activities, we continue with the next user.
                2. We parse the trackpoints from the .plt files for this user. 
                   We skip those .plt files that contain more than 2,500 trackpoints.
                3. We use the find_matching_activities function find the corresponding activity for each trackpoint.
                   If we don't find a activity at the timestamp of the trackpoint, we don't add the trackpoint to the database.
                4. We add all the relecant trackpoints to the MongoDB collection.



        Args:
            collection_name (string): The name of the collection we want to insert the data into. Defaults to "TrackPoint".
        """        


        print("----------------------------------")
        print("Parsing and adding Trackpoints to the Database")       
        print("----------------------------------")
        for folder in tqdm(os.listdir(self.base_path)):

            #Step 1:

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
        """This is a helper function which allows us to efficiently find the matching activities for a given trackpoint.

        Args:
            activities (list): A list with all the rows of the activities for a given user.
            data (list): A list with all the necassary information about the trackpoints. This function then adds the activities_ids to the data.
        """     
        
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
        
        
    except Exception as e:
        # program.drop_coll(collection_name='User')
        # program.drop_coll(collection_name='Activity')
        # program.drop_coll(collection_name='TrackPoint')

        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
