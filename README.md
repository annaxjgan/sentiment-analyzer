# Sentiment Analyzer
An MPI-based tool for analyzing sentiment data from large NDJSON files.
Structure
The application is organized into several modules:

__main__.py: Entry point for the application
data_io.py: Functions for reading and parsing data
utils.py: Utility functions like date parsing
processors.py: Core data processing logic
mpi_controller.py: MPI distribution and collection logic

Installation
bashCopypip install -e .
Usage
bashCopympiexec -n <num_processes> python -m sentiment_analyzer <file_path>
Where:

<num_processes> is the number of MPI processes to use
<file_path> is the path to your NDJSON data file

Requirements

Python 3.6+
mpi4py