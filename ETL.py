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

section_df = data[[
    'Class Nbr', 'Component', 'Room Capacity', 'Combined?', 'Waitlist Capacity',
    'Waitlist Total', 'Prgrss Unt', 'Class Stat', 'Session', 'Instruction Mode'
]].drop_duplicates().copy()

section_df.rename(columns={
    'Class Nbr': 'SectionClassID',
    'Component': 'Component',
    'Room Capacity': 'Room',
    'Combined?': 'isCombined',
    'Waitlist Capacity': 'WaitlistCapacity',
    'Waitlist Total': 'WaitlistTotal',
    'Prgrss Unt': 'ProgressUnit',
    'Class Stat': 'StatusID',
    'Session': 'Session',
    'Instruction Mode': 'InstructionMode'
}, inplace=True)

section_df['isCombined'] = section_df['isCombined'].map({'Yes': True, 'No': False})

# print(section_df)

# Create InstructorID
data['InstructorFullName'] = data['Instructor First Name'].str.strip() + ' ' + data['Instructor Last Name'].str.strip()
instructors = data[['InstructorFullName']].drop_duplicates().reset_index(drop=True)
instructors['InstructorID'] = instructors.index + 1  # starting IDs from 1

# Merge InstructorID into the original DataFrame
data = data.merge(instructors, on='InstructorFullName', how='left')

data['SectionClassID'] = data['Class Nbr']

# Keep only relevant columns
section_instructor = data[['SectionClassID', 'InstructorID']].drop_duplicates()

section_instructor = section_instructor.sort_values(by=['SectionClassID', 'InstructorID']).reset_index(drop=True)

# print(section_instructor_df)

import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('ClassSchedule.db')

# Save dataframes to the database as tables
college_df.to_sql('College', conn, if_exists='replace', index=False)
instructor_df.to_sql('Instructor', conn, if_exists='replace', index=False)
room_df.to_sql('Room', conn, if_exists='replace', index=False)
status_df.to_sql('Status', conn, if_exists='replace', index=False)
class_df.to_sql('Class', conn, if_exists='replace', index=False)
section_df.to_sql('Section', conn, if_exists='replace', index=False)
section_instructor.to_sql('SectionInstructor', conn, if_exists='replace', index=False)


# List all tables
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)

# Loop through each table and print its contents
for table_name in tables['name']:
    print(f"\nContents of {table_name} table:")
    table_data = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    print(table_data)

conn.close()

