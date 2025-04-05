import sys
from mpi4py import MPI
from data_io import read_ndjson_in_chunks
from processors import process_chunk
from mpi_controller import distribute_and_collect_results

def main():
    """
    Main entry point for the application
    """
    if len(sys.argv) != 2:
        print("Not enough arguments. Usage: python -m sentiment_analyzer <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Start timer
    start_time = MPI.Wtime()
    
    # Distribute work and collect results
    if rank == 0:
        final_results = distribute_and_collect_results(comm, size, file_path)
        
        # End timer
        end_time = MPI.Wtime()
        print(f"\nTotal Execution Time: {end_time - start_time:.3f} * 1000 milliseconds")
    else:
        # Worker processes execute their part
        distribute_and_collect_results(comm, size, None)

if __name__ == "__main__":
    main()