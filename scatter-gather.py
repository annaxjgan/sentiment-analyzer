import json
import sys
import heapq
from mpi4py import MPI
from collections import defaultdict
from datetime import datetime

def read_ndjson_in_chunks(file_path, chunk_size=1000):
    """Generator function to read ndjson file in chunks"""
    try:
        # Open the file in read mode
        with open(file_path, 'r') as f:
            chunk = [] # Initialize an empty list to hold the chunk of records
            # Read the file line by line
            for line in f:
                # Parse current line of the file as a json object
                chunk.append(json.loads(line))
                # If the chunk size is reached, yield the chunk
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            if chunk:  # Yield any remaining records
                yield chunk

    except FileNotFoundError:
        print(f"File {file_path} not found.")
        sys.exit(1)

    except Exception as e:
        print(f"Error reading file: {file_path}, Error: {e}")
        sys.exit(1)

def parse_date(timestamp):
    """Function to parse the date string and return a formatted date string"""
    try:
        # Parse the timestamp and convert to date string
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        datetime_str = dt.date().isoformat()
        hour = dt.hour
        date_str = datetime_str + " " + f"{hour}:00-{hour+1}:00"
        return date_str
    except ValueError as e:
        # Skip processing for invalid timestamps
        return None

def process_chunk(chunk):
    """Process a chunk of records and return sentiment aggregations"""
    hour_sentiments = defaultdict(float)
    user_sentiments = defaultdict(float)

    for record in chunk:
        # Skip if the record is not properly formatted
        if not isinstance(record, dict):
            continue

        # Extract the document
        obj = record.get('doc')
        if not isinstance(obj, dict) or not obj:
            continue

        # Check if 'account' exists and has required fields
        account = obj.get('account')
        if not isinstance(account, dict) or not account:
            continue
            
        # Extract relevant fields
        timestamp = obj.get('createdAt')
        sentiment = obj.get('sentiment')
        account_id = account.get('id')
        account_name = account.get('username')
        
        # Skip records with missing or invalid data
        if (not isinstance(timestamp, str) or not timestamp or
            not isinstance(sentiment, (int, float)) or
            not isinstance(account_id, str) or not account_id or
            not isinstance(account_name, str) or not account_name):
            continue

        # Parse date time
        hour = parse_date(timestamp)
        if hour is None:
            continue
        
        # Aggregate sentiment data
        hour_sentiments[hour] += sentiment
        user_sentiments[(account_name, account_id)] += sentiment

    return dict(hour_sentiments), dict(user_sentiments)


def main(file_path):
    """Main function to initialize MPI and process the file"""

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Start timer
    start_time = MPI.Wtime()

    final_time_score_data = defaultdict(float)
    final_user_score_data = defaultdict(float)
    
    time_score_list = []  # to store worker time score results
    user_score_list = []  # to store worker user score results


    # Master process reads the data and distributes chunks
    if rank == 0:
        # Read data chunks from file
        chunk_index = 0
        for chunk in read_ndjson_in_chunks(file_path):
            chunk_index += 1
            chunk_size = len(chunk)
        
            print(f"Processing chunk {chunk_index} of size: {chunk_size}")
            
            # Create equal sized sub-chunks for each process
            sub_chunk_size = max(1, chunk_size // size)
            chunks = [chunk[i:i+sub_chunk_size] for i in range(0, chunk_size, sub_chunk_size)]
            
            # If we have more chunks than processes, add extras to the last chunk
            if len(chunks) > size:
                chunks[size-1:] = [sum(chunks[size-1:], [])]
                chunks = chunks[:size]
            
            # If we have fewer chunks than processes, add empty chunks
            while len(chunks) < size:
                chunks.append([])
                
            # Scatter chunks to all processes
            my_chunk = comm.scatter(chunks, root=0)
            
            # Process local chunk
            local_time_scores, local_user_scores = process_chunk(my_chunk)
            
            # Gather results from all processes
            # blocks until all processes have sent their data and called the gather function
            all_time_scores = comm.gather(local_time_scores, root=0)
            all_user_scores = comm.gather(local_user_scores, root=0)
            
            # Aggregate results
            for time_scores in all_time_scores:
                for timestamp, score in time_scores.items():
                    final_time_score_data[timestamp] += score
            
            for user_scores in all_user_scores:
                for user_key, score in user_scores.items():
                    final_user_score_data[user_key] += score
        
        # Send termination signal (empty chunks) to all processes
        empty_chunks = [[] for _ in range(size)]
        comm.scatter(empty_chunks, root=0)

        # Get the top 5 happiest hours (largest sentiment scores)
        happiest_hours = heapq.nlargest(5, final_time_score_data.items(), key=lambda x: x[1])
        # Get the bottom 5 saddest hours (smallest sentiment scores)
        saddest_hours = heapq.nsmallest(5, final_time_score_data.items(), key=lambda x: x[1])
        # Get the top 5 happiest users (largest sentiment scores)
        happiest_users = heapq.nlargest(5, final_user_score_data.items(), key=lambda x: x[1])
        # Get the bottom 5 saddest users (smallest sentiment scores)
        saddest_users = heapq.nsmallest(5, final_user_score_data.items(), key=lambda x: x[1])

        # Print top five sentiment score
        print('\n=======Top 5 happiest hours=========')
        for key, value in happiest_hours:
            print(f'Created time: {key}, Total sentiment score: {value}')

        # Print bottom five sentiment score
        print('\n=======Top 5 saddest hours=========')
        for key, value in saddest_hours:
            print(f'Created time: {key}, Total sentiment score: {value}')

        # Print happiest people
        print('=======Top 5 happiest people=========')
        for item in happiest_users:
            account_id, account_name = item[0] 
            value = item[1]
            print(f'Account id: {account_id}, Username: {account_name}, Total sentiment score: {value}')

        # Print saddest people
        print('\n=======Top 5 saddest people=========')
        for item in saddest_users:
            account_id, account_name = item[0]
            value = item[1]
            print(f'Account id: {account_id}, Username: {account_name}, Total sentiment score: {value}')

        # End timer
        end_time = MPI.Wtime()
        print(f"\nTotal Execution Time: {end_time - start_time:.3f} * 1000 milliseconds")

        sys.stdout.flush()
        sys.stderr.flush()

    else:
        while True:
            worker_chunk = comm.scatter(None, root=0) # scatter empty chunks
            # Break if receive
            if not worker_chunk: 
                break
            # Process local chunk
            local_time_scores, local_user_scores = process_chunk(worker_chunk)
            # Gather results from all processes
            comm.gather(local_time_scores, root=0)
            comm.gather(local_user_scores, root=0)


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Not enough arguments. Usage: python script.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    main(file_path)
