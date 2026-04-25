import os
import requests
import zipfile
import pandas as pd
import tempfile
import shutil

MOVIELENS_1M_URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
MOVIELENS_1M_FILENAME = "ml-1m.zip"

def download_movielens(output_dir):
    zip_path = os.path.join(output_dir, MOVIELENS_1M_FILENAME)
    
    if os.path.exists(zip_path):
        print(f"MovieLens-1M zip file already exists: {zip_path}")
    else:
        print(f"Downloading MovieLens-1M dataset from {MOVIELENS_1M_URL}...")
        response = requests.get(MOVIELENS_1M_URL, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Download completed: {zip_path}")
    
    return zip_path

def extract_movielens(zip_path, output_dir):
    extract_dir = os.path.join(output_dir, "ml-1m")
    
    if os.path.exists(extract_dir):
        print(f"MovieLens-1M already extracted: {extract_dir}")
    else:
        print(f"Extracting {zip_path}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"Extraction completed: {extract_dir}")
    
    return extract_dir

def parse_users_dat(users_file):
    print(f"Parsing users.dat: {users_file}")
    users = []
    with open(users_file, 'r', encoding='latin-1') as f:
        for line in f:
            parts = line.strip().split('::')
            if len(parts) == 5:
                users.append({
                    'user_id': int(parts[0]),
                    'gender': parts[1],
                    'age': int(parts[2]),
                    'occupation': int(parts[3]),
                    'zip_code': parts[4]
                })
    df = pd.DataFrame(users)
    df['age'] = df['age'].astype('int32')
    df['occupation'] = df['occupation'].astype('int32')
    print(f"Parsed {len(df)} user records")
    return df

def parse_movies_dat(movies_file):
    print(f"Parsing movies.dat: {movies_file}")
    movies = []
    with open(movies_file, 'r', encoding='latin-1') as f:
        for line in f:
            parts = line.strip().split('::')
            if len(parts) == 3:
                movies.append({
                    'movie_id': int(parts[0]),
                    'title': parts[1],
                    'genres': parts[2].split('|')
                })
    df = pd.DataFrame(movies)
    print(f"Parsed {len(df)} movie records")
    return df

def parse_ratings_dat(ratings_file):
    print(f"Parsing ratings.dat: {ratings_file}")
    ratings = []
    with open(ratings_file, 'r', encoding='latin-1') as f:
        for line in f:
            parts = line.strip().split('::')
            if len(parts) == 4:
                ratings.append({
                    'user_id': int(parts[0]),
                    'movie_id': int(parts[1]),
                    'rating': (float(parts[2]) - 1) / 4,
                    'timestamp': int(parts[3])
                })
    df = pd.DataFrame(ratings)
    df['rating'] = df['rating'].astype('float32')
    print(f"Parsed {len(df)} rating records")
    return df

def save_to_parquet(df, output_path):
    df.to_parquet(output_path, index=False)
    print(f"Saved parquet file: {output_path} ({len(df)} records)")

def main():
    output_dir = os.path.dirname(os.path.realpath(__file__))
    
    zip_path = download_movielens(output_dir)
    
    extract_dir = extract_movielens(zip_path, output_dir)
    
    users_file = os.path.join(extract_dir, "users.dat")
    movies_file = os.path.join(extract_dir, "movies.dat")
    ratings_file = os.path.join(extract_dir, "ratings.dat")
    
    users_df = parse_users_dat(users_file)
    movies_df = parse_movies_dat(movies_file)
    ratings_df = parse_ratings_dat(ratings_file)
    
    users_parquet = os.path.join(output_dir, "ml_users.parquet")
    movies_parquet = os.path.join(output_dir, "ml_movies.parquet")
    ratings_parquet = os.path.join(output_dir, "ml_ratings.parquet")
    
    save_to_parquet(users_df, users_parquet)
    save_to_parquet(movies_df, movies_parquet)
    save_to_parquet(ratings_df, ratings_parquet)
    
    print("\n=== MovieLens-1M dataset processing completed! ===")
    print(f"Users: {users_parquet}")
    print(f"Movies: {movies_parquet}")
    print(f"Ratings: {ratings_parquet}")

if __name__ == "__main__":
    main()
