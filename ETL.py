import pandas as pd
import numpy as np
import uuid

#extracts data from excel file into a pandas dataframe 
data = pd.read_excel('sample ClassSched-CS-S25.xlsx', header=1)

#lool at the data
# print(data)

#populates dataframe for college_df, creating unique id for each row
college_df = data[['College']].drop_duplicates().rename(columns={'College': 'CollegeName'})
college_df['CollegeID'] = range(1, len(college_df) + 1)

# print(college_df)


#populates dataframe for instructor_df, creating unique id for each row
instructor_df = data[['Instructor First Name', 'Instructor Last Name']].drop_duplicates().rename(columns={
                                                                                                'Instructor First Name': 'InstructorFirstName', 
                                                                                                'Instructor Last Name': 'InstructorLastName'})
instructor_df['InstructorID'] = range(1, len(instructor_df) + 1)

# print(instructor_df)


#populates dataframe for room_df, creating unique id for each row
room_df = data[['Room', 'Room Capacity']].drop_duplicates().rename(columns={'Room': 'RoomNo', 'Room Capacity': 'RoomCapacity'})

room_df['RoomID'] = range(1, len(room_df) + 1)

# print(room_df)

status_df = data[['Class Stat']].drop_duplicates().rename(columns={'Class Stat': 'StatusCode'})

status_df['StatusID'] = range(1, len(status_df) + 1)

# print(status_df)


#populates dataframe for class
class_df = data[['Title', 'Catalog', 'Subject', 'Section', 'Enrollment Capacity', 'College']].drop_duplicates().rename(columns={'Enrollment Capacity': 'Enrollment_Capacity'})
class_df['College_ID'] = college_df['CollegeID']

#adds CollegeID to class_df from college_df
class_df = class_df.merge(college_df[['CollegeName', 'CollegeID']], 
                          
                          #Join conditions
                          left_on='College', 
                          right_on='CollegeName', 
                          
                          #join type
                          how='left')

class_df = class_df.drop('CollegeName', axis=1)

# print(class_df)