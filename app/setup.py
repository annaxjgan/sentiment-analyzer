from setuptools import setup, find_packages

setup(
    name="app",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "mpi4py",
    ],
    entry_points={ 
        "console_scripts": [ # tells pip to create executable CLI scripts that can be run from the command line
            # The format is <script_name>=<module_name>:<function_name>
            "sentiment_analyzer=__main__:main",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="MPI-based sentiment analysis tool",
)