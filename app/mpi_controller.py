import sys
from collections import defaultdict
from data_io import read_ndjson_in_chunks
from processors import process_chunk, display_results

def distribute_and_collect_results(comm, size, file_path):
    """
    Distribute work among processes and collect results
    :param comm: MPI communicator
    :param size: Number of processes
    :param file_path: Path to the ndjson file (only used by master process)
    :return: Final aggregated results (only by master process)
    """
    rank = comm.Get_rank()
    
    # Master process reads the data and distributes chunks
    if rank == 0:
        final_time_score_data = defaultdict(float)
        final_user_score_data = defaultdict(float)
        
        # Read data chunks from file
        for chunk in read_ndjson_in_chunks(file_path):
            chunk_size = len(chunk)
                    
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

        # Display the results
        display_results(final_time_score_data, final_user_score_data)

        # Ensure everything is printed before exiting
        sys.stdout.flush()
        sys.stderr.flush()
        
        return (final_time_score_data, final_user_score_data)

    else:  # Worker processes
        while True:
            worker_chunk = comm.scatter(None, root=0)  # receive chunk from master
            
            # Break if receive empty chunk (termination signal)
            if not worker_chunk:
                break
                
            # Process local chunk
            local_time_scores, local_user_scores = process_chunk(worker_chunk)
            
            # Send results back to master
            comm.gather(local_time_scores, root=0)
            comm.gather(local_user_scores, root=0)
        
        return None