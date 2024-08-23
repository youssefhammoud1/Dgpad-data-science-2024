import pymongo
import json

# Connect to MongoDB
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["almayadeen"]
    collection = db["articles"]
    print("MongoDB connected successfully!")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit()

# List of JSON file names
files = [
    'output/articles_2024_8.xml.json',
    'output/articles_2024_7.xml.json',
    'output/articles_2024_6.xml.json',
    'output/articles_2024_5.xml.json',
    'output/articles_2024_4.xml.json',
    'output/articles_2024_3.xml.json',
    # Add more file paths as needed
]

# Load and insert JSON data
try:
    for file_path in files:
        print(f"Processing file: {file_path}")

        with open(file_path, encoding='utf-8') as f:
            data = json.load(f)

            # Ensure data is a list of dictionaries
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                collection.insert_many(data)
                print(f"Data from {file_path} inserted successfully!")
            else:
                print(f"The JSON data in {file_path} is not in the expected format. It should be a list of dictionaries.")
except FileNotFoundError:
    print("One or more files were not found. Please check the file paths and names.")
except json.JSONDecodeError:
    print("Error decoding JSON. Please ensure all files contain valid JSON.")
except UnicodeDecodeError:
    print("Error decoding one or more files. Please check the files' encodings.")
except Exception as e:
    print(f"An error occurred: {e}")