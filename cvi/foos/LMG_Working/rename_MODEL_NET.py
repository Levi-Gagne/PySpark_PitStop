
# Imports
import os, re
from datetime import datetime, timedelta


# Print the current working directory
current_directory = os.getcwd()
print("Current working directory:", current_directory)


# Directory of files to rename
directory = '/home/datascientist/Monday'


# Regex pattern to match the file format MODEL_NET_YYYYMMDD.txt
pattern   = re.compile(r'MODEL_NET_(\d{8})\.txt')


# Iterate over the files in the directory and rename them
for filename in os.listdir(directory):
    match = pattern.match(filename)

    if match:
        # Extract the date part from the filename
        date_str      = match.group(1)
        file_date     = datetime.strptime(date_str, '%Y%m%d')

        # Add one day to the date
        new_date      = file_date + timedelta(days=1)
        new_date_str  = new_date.strftime('%Y%m%d')

        # Create the new filename
        new_filename  = f'MODEL_NET_{new_date_str}.txt'

        # Construct the full old and new file paths
        old_file_path = os.path.join(directory, filename)
        new_file_path = os.path.join(directory, new_filename)

        # Rename the file and print the action
        os.rename(old_file_path, new_file_path)
        print(f'Renamed "{filename}" to "{new_filename}"')

print("File renaming completed.")