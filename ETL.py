import pandas as pd
import numpy as np
import sqlite3

# Load data
data = pd.read_excel('sample ClassSched-CS-S25.xlsx', header=1)

# Connect to DB early to allow deduplication (prevent every rerun to always dupe the data)
conn = sqlite3.connect('ClassSchedule.db')

# --- COLLEGE ---
college_df = data[['College']].drop_duplicates().rename(columns={'College': 'CollegeName'})
existing_colleges = pd.read_sql("SELECT CollegeName FROM College;", conn)
college_df = college_df[~college_df['CollegeName'].isin(existing_colleges['CollegeName'])]
college_df['CollegeID'] = range(1, len(college_df) + 1)

# --- INSTRUCTOR ---
instructor_df = data[['Instructor First Name', 'Instructor Last Name']].drop_duplicates().rename(columns={
    'Instructor First Name': 'InstructorFirstName',
    'Instructor Last Name': 'InstructorLastName'
})
existing_instructors = pd.read_sql("SELECT InstructorFirstName, InstructorLastName FROM Instructor;", conn)
instructor_df = instructor_df.merge(existing_instructors, on=['InstructorFirstName', 'InstructorLastName'], how='left', indicator=True)
instructor_df = instructor_df[instructor_df['_merge'] == 'left_only'].drop(columns=['_merge'])
instructor_df['InstructorID'] = range(1, len(instructor_df) + 1)

# --- ROOM ---
room_df = data[['Room', 'Room Capacity']].drop_duplicates().rename(columns={
    'Room': 'RoomNo',
    'Room Capacity': 'RoomCapacity'
})
existing_rooms = pd.read_sql("SELECT RoomNo, RoomCapacity FROM Room;", conn)
room_df = room_df.merge(existing_rooms, on=['RoomNo', 'RoomCapacity'], how='left', indicator=True)
room_df = room_df[room_df['_merge'] == 'left_only'].drop(columns=['_merge'])
room_df['RoomID'] = range(1, len(room_df) + 1)

# --- STATUS ---
status_df = data[['Class Stat']].drop_duplicates().rename(columns={'Class Stat': 'StatusCode'})
existing_statuses = pd.read_sql("SELECT StatusCode FROM Status;", conn)
status_df = status_df[~status_df['StatusCode'].isin(existing_statuses['StatusCode'])]
status_df['StatusID'] = range(1, len(status_df) + 1)

# --- CLASS ---
class_df = data[['Title', 'Catalog', 'Subject', 'Section', 'Enrollment Capacity', 'College']].drop_duplicates()
class_df = class_df.rename(columns={'Enrollment Capacity': 'Enrollment_Capacity'})

# Get existing College IDs
college_ids = pd.read_sql("SELECT * FROM College;", conn)
class_df = class_df.merge(college_ids, left_on='College', right_on='CollegeName', how='left')
class_df.drop(columns=['College', 'CollegeName'], inplace=True)

class_df['Term'] = 'Spring 2025'
class_df = class_df[['Title', 'Catalog', 'Subject', 'Section', 'Enrollment_Capacity', 'CollegeID', 'Term']]

existing_class = pd.read_sql("SELECT Title, Catalog, Subject, Section, Term FROM Class;", conn)
class_df = class_df.merge(existing_class, on=['Title', 'Catalog', 'Subject', 'Section', 'Term'], how='left', indicator=True)
class_df = class_df[class_df['_merge'] == 'left_only'].drop(columns=['_merge'])

# --- SECTION ---
section_df = data[['Class Nbr', 'Component', 'Combined?', 'Waitlist Capacity',
                   'Waitlist Total', 'Prgrss Unt', 'Class Stat', 'Session', 'Instruction Mode']].drop_duplicates().copy()

section_df.rename(columns={
    'Class Nbr': 'SectionClassID',
    'Component': 'Component',
    'Combined?': 'IsCombined',
    'Waitlist Capacity': 'WaitlistCapacity',
    'Waitlist Total': 'WaitlistTotal',
    'Prgrss Unt': 'ProgressUnit',
    'Class Stat': 'StatusID',
    'Session': 'Session',
    'Instruction Mode': 'InstructionMode'
}, inplace=True)

section_df['IsCombined'] = section_df['IsCombined'].map({'Yes': True, 'No': False})
section_df['ClassDays'] = 'TBD'
section_df['StartTime'] = '00:00'
section_df['EndTime'] = '00:00'
section_df['StartDate'] = '2025-01-01'
section_df['EndDate'] = '2025-05-01'
section_df['RoomID'] = 1

section_df = section_df[['SectionClassID', 'ClassDays', 'StartTime', 'EndTime', 'StartDate', 'EndDate',
                         'RoomID', 'Component', 'IsCombined', 'WaitlistCapacity', 'WaitlistTotal',
                         'ProgressUnit', 'StatusID', 'Session', 'InstructionMode']]

existing_section_ids = pd.read_sql("SELECT SectionClassID FROM Section;", conn)
section_df = section_df[~section_df['SectionClassID'].isin(existing_section_ids['SectionClassID'])]

# --- SECTION INSTRUCTOR ---
data['InstructorFullName'] = data['Instructor First Name'].str.strip() + ' ' + data['Instructor Last Name'].str.strip()
instructors = data[['InstructorFullName']].drop_duplicates().reset_index(drop=True)
instructors['InstructorID'] = instructors.index + 1

data = data.merge(instructors, on='InstructorFullName', how='left')
data['SectionClassID'] = data['Class Nbr']
section_instructor = data[['SectionClassID', 'InstructorID']].drop_duplicates()

existing_section_instructor = pd.read_sql("SELECT SectionClassID, InstructorID FROM SectionInstructor;", conn)
section_instructor = section_instructor.merge(existing_section_instructor, on=['SectionClassID', 'InstructorID'], how='left', indicator=True)
section_instructor = section_instructor[section_instructor['_merge'] == 'left_only'].drop(columns=['_merge'])

# --- SAVE TO DATABASE ---
college_df.to_sql('College', conn, if_exists='append', index=False)
instructor_df.to_sql('Instructor', conn, if_exists='append', index=False)
room_df.to_sql('Room', conn, if_exists='append', index=False)
status_df.to_sql('Status', conn, if_exists='append', index=False)
class_df.to_sql('Class', conn, if_exists='append', index=False)
section_df.to_sql('Section', conn, if_exists='append', index=False)
section_instructor.to_sql('SectionInstructor', conn, if_exists='append', index=False)

# Print contents of each table
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)

for table_name in tables['name']:
    print(f"\nContents of {table_name} table:")
    table_data = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    print(table_data)

conn.close()
