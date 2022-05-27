from pathlib import Path


def generate_query(input_file: Path): 

    with open(input_file, "r") as f: 
        query = f.read()
    
    return query
