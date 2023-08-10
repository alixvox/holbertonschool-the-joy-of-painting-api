import pandas as pd
import sqlite3
from datetime import datetime
import re

log_file_path = "log_joy_of_painting.txt"
error_log_file_path = "log_errors_joy_of_coding.txt"

# Function to initialize the log files
def initialize_log_files():
    with open(log_file_path, 'a') as log_file:
        log_file.write("\n" + "="*50 + "\n")
        log_file.write(f"ETL Process Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write("="*50 + "\n\n")

    with open(error_log_file_path, 'a') as error_log_file:
        error_log_file.write("\n" + "="*50 + "\n")
        error_log_file.write(f"ETL Process Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        error_log_file.write("="*50 + "\n\n")

# Combined extraction and transformation function for Episode Dates
def extract_and_transform_episode_dates(file_path):
    # Lists to store extracted data
    titles = []
    broadcast_dates = []

    # Regular expression pattern to match the episode title and broadcast date
    pattern = r'"(.*?)" \((.*?)\)'
    
    # Open the file and read line by line
    with open(file_path, 'r') as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                title = match.group(1).lower()  # Convert title to lowercase
                date_str = match.group(2)
                
                # Convert date to SQLite compatible format
                try:
                    date_obj = datetime.strptime(date_str, '%B %d, %Y')
                    sqlite_date = date_obj.strftime('%Y-%m-%d')
                    
                    titles.append(title)
                    broadcast_dates.append(sqlite_date)
                except ValueError:
                    # Log error if date conversion fails
                    with open(error_log_file_path, 'a') as error_log:
                        error_log.write(f"Error converting date for episode: {title}\n")
            else:
                # Log error if pattern match fails
                with open(error_log_file_path, 'a') as error_log:
                    error_log.write(f"Error extracting data from line: {line}\n")

    # Convert lists to DataFrame
    df = pd.DataFrame({
        'Title': titles,
        'BroadcastDate': broadcast_dates
    })

    # Log completion
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Completed processing file: {file_path}\n")

    return df


# Placeholder for the other combined extraction and transformation functions
def extract_and_transform_subject_matter(file_path):
    # Lists to store extracted data
    titles = []
    subjects = []

    # Regular expression pattern to match the episode title and subject matter
    pattern = r'"(.*?)" \((.*?)\)'
    
    # Open the file and read line by line
    with open(file_path, 'r') as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                title = match.group(1).lower()  # Convert title to lowercase
                subject = match.group(2)
                
                titles.append(title)
                subjects.append(subject)
            else:
                # Log error if pattern match fails
                with open(error_log_file_path, 'a') as error_log:
                    error_log.write(f"Error extracting data from line: {line}\n")

    # Convert lists to DataFrame
    df = pd.DataFrame({
        'Title': titles,
        'SubjectMatter': subjects
    })

    # Log completion
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Completed processing file: {file_path}\n")

    return df

def extract_and_transform_colors_used(file_path):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Ensure the required columns are present
        if 'painting_title' not in df.columns or 'colors' not in df.columns:
            raise ValueError("Required columns not found in the dataset.")

        # Convert the painting_title to lowercase
        df['painting_title'] = df['painting_title'].str.lower()

        # Extract colors from the colors column
        df['colors'] = df['colors'].apply(lambda x: re.findall(r"'(.*?)'", x))

        # Create a new DataFrame to store the episode titles and their associated colors
        colors_df = pd.DataFrame(columns=['painting_title', 'color'])

        for index, row in df.iterrows():
            for color in row['colors']:
                colors_df = colors_df.append({'painting_title': row['painting_title'], 'color': color}, ignore_index=True)

        # Log the successful processing of the file
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Successfully processed {file_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        return colors_df

    except Exception as e:
        # Log any errors
        with open(error_log_file_path, 'a') as error_log_file:
            error_log_file.write(f"Error processing {file_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {str(e)}\n")
        return None

def validate_dataframe(df, required_columns):
    """
    Validates the dataframe for the presence of required columns and checks for missing values.
    
    Parameters:
    - df: The dataframe to validate.
    - required_columns: A list of columns that must be present in the dataframe.
    
    Returns:
    - True if the dataframe is valid, False otherwise.
    """
    # Check for the presence of required columns
    for column in required_columns:
        if column not in df.columns:
            print(f"Error: Column '{column}' not found in the dataframe.")
            return False

    # Check for missing values in the required columns
    for column in required_columns:
        if df[column].isnull().any():
            print(f"Error: Missing values detected in column '{column}'.")
            return False

    return True

# Placeholder for the loading function
def load_data_into_db(db_path):
    # Extract and transform data from each dataset
    episode_dates_df = extract_and_transform_episode_dates(episode_dates_path)
    subject_matter_df = extract_and_transform_subject_matter(subject_matter_path)
    colors_used_df = extract_and_transform_colors_used(colors_used_path)

    # Validate the dataframes
    if not validate_dataframe(episode_dates_df, ['Title', 'BroadcastDate']):
        print("Validation failed for episode_dates_df.")
        return

    if not validate_dataframe(subject_matter_df, ['painting_title', 'subject']):
        print("Validation failed for subject_matter_df.")
        return

    if not validate_dataframe(colors_used_df, ['painting_title', 'color']):
        print("Validation failed for colors_used_df.")
        return

    # Merge the dataframes to create a consolidated dataframe for the Episode table
    merged_df = episode_dates_df.merge(subject_matter_df, left_on='Title', right_on='painting_title', how='left')
    merged_df = merged_df.merge(colors_used_df, on='painting_title', how='left')

    # Establish a connection to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert data into the Episode table
    for index, row in merged_df.iterrows():
        cursor.execute("""
            INSERT INTO Episode (Title, BroadcastDate, ImageSrc, YouTubeSrc, Season, EpisodeInSeason, TotalEpisodeNum)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (row['Title'], row['BroadcastDate'], row['ImageSrc'], row['YouTubeSrc'], row['Season'], row['EpisodeInSeason'], row['TotalEpisodeNum']))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    # Log the successful loading of data
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Data successfully loaded into {db_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Main function
def main():
    # Initialize the log files
    initialize_log_files()

    # File paths for the datasets
    episode_dates_path = "The Joy of Coding - Episode Dates"
    subject_matter_path = "The Joy of Coding - Subject Matter"
    colors_used_path = "The Joy of Coding - Colors Used"

    # Database path
    db_path = "joy_of_painting.db"

    # Combined Extraction and Transformation
    extract_and_transform_episode_dates(episode_dates_path)
    extract_and_transform_subject_matter(subject_matter_path)
    extract_and_transform_colors_used(colors_used_path)

    # Loading
    load_data_into_db(db_path)

    # Print a summary or any other final steps if needed
    print("ETL process completed!")

if __name__ == "__main__":
    main()
