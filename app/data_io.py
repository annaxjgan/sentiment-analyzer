import json
import sys

def read_ndjson_in_chunks(file_path, chunk_size=7000):
    """
    Generator function to read ndjson file in chunks
    :param file_path: Path to the ndjson file
    :param chunk_size: Number of records to read in each chunk
    :return: Yields chunks of records
    """
    try:
        # Open the file in read mode
        with open(file_path, 'r') as f:
            chunk = []  # Initialize an empty list to hold the chunk of records
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