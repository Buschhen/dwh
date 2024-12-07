import math
import os
import h5py
import csv
from concurrent.futures import ThreadPoolExecutor

root_directory = './MillionSongSubset'
output = './msd.csv'
header = [ 'duration', 'end_of_fade_in', 'key', 'loudness', 'mode', 'start_of_fade_out', 'tempo', 'time_signature', 'artist_familiarity', 'artist_hotttnesss','song_hotttnesss' , 'title']


def init_csv_file(file, header):
    """Initialize the CSV file with the provided header."""
    with open(file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)


def process_file(file_path):
    """Extract relevant data from a single .h5 file."""
    try:
        with h5py.File(file_path, 'r') as h5_file:
            analysis_songs = h5_file['/analysis/songs/'][0]
            metadata_songs = h5_file['/metadata/songs/'][0]

            data = [
                analysis_songs[3],  # duration
                analysis_songs[4],  # end_of_fade_in
                analysis_songs[21], # key
                analysis_songs[23], # loudness
                analysis_songs[24], # mode
                analysis_songs[26], # start_of_fade_out
                analysis_songs[27], # tempo
                analysis_songs[28], # time_signature
                metadata_songs[2],  # artist_familiarity
                metadata_songs[3],  # artist_hotttnesss
                metadata_songs[16], # song_hotttnesss
                metadata_songs[18]  #title
            ]
            data = [0.0 if (isinstance(x, float) and math.isnan(x)) else x for x in data]

            return data
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def h5_to_csv(dir):
    """Process all .h5 files in the directory and write to CSV in one go."""
    files_to_process = []
    for subdir, _, files in os.walk(dir):
        for file in files:
            if file.endswith('.h5'):
                files_to_process.append(os.path.join(subdir, file))

    # Use multithreading to process files
    num_threads = 120
    all_data = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(process_file, files_to_process)
        all_data.extend(filter(None, results)) 

    with open(output, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(all_data)


if __name__ == "__main__":
    init_csv_file(output, header)
    h5_to_csv(root_directory)