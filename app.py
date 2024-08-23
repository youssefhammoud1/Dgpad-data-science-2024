from flask import Flask, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import re

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["almayadeen"]
collection = db["articles"]

# Route for getting top keywords
@app.route('/top_keywords', methods=['GET'])
def top_keywords():
    pipeline = [
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$keywords", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)
# Route for getting top authors
@app.route('/top_authors', methods=['GET'])
def top_authors():
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# Route for getting articles by publication date
@app.route('/articles_by_date', methods=['GET'])
def articles_by_date():
    pipeline = [
        {"$match": {"published_time": {"$ne": None}}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$published_time"}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))

    # Format the result
    formatted_result = {item['_id']: item['count'] for item in result}

    return jsonify(formatted_result)

# Route for getting articles by word count
@app.route('/articles_by_word_count', methods=['GET'])
def articles_by_word_count():
    pipeline = [
        {"$group": {
            "_id": "$word_count",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    result = list(collection.aggregate(pipeline))

    # Format the result
    formatted_result = {f"{item['_id']} words": item['count'] for item in result}

    return jsonify(formatted_result)


@app.route('/articles_by_language', methods=['GET'])
def articles_by_language():
    pipeline = [
        {"$group": {
            "_id": "$language",  # Group by the language field
            "count": {"$sum": 1}  # Count the number of articles per language
        }},
        {"$sort": {"_id": 1}}  # Sort by language alphabetically
    ]

    result = list(collection.aggregate(pipeline))

    # Format the result
    formatted_result = {f"{item['_id']}": item['count'] for item in result}

    return jsonify(formatted_result)


@app.route('/articles_by_classes', methods=['GET'])
def articles_by_classes():
    pipeline = [
        {"$group": {
            "_id": "$classes",  # Group by the classes field
            "count": {"$sum": 1}  # Count the number of articles per class
        }},
        {"$sort": {"_id": 1}}  # Sort by class in ascending order
    ]

    result = list(collection.aggregate(pipeline))

    # Format the result
    formatted_result = {f"{item['_id']}": item['count'] for item in result}

    return jsonify(formatted_result)


def format_date(date):
    """Helper function to format the publication date."""
    today = datetime.now(timezone.utc).date()  # Use timezone-aware datetime
    date = date.date()

    if date == today:
        return "Published today"
    elif date == today - timedelta(days=1):
        return "Published yesterday"
    else:
        return f"Published on {date.strftime('%Y-%m-%d')}"


@app.route('/recent_articles', methods=['GET'])
def recent_articles():
    # Find the 10 most recently published articles
    articles = collection.find({}).sort("published_time", -1).limit(10)

    # Format the result
    result = {}
    for article in articles:
        title = article.get('title', 'No Title')
        published_time = article.get('published_time', datetime.now(timezone.utc))
        result[title] = format_date(published_time)

    return jsonify(result)


@app.route('/articles_by_keyword/<keyword>', methods=['GET'])
def articles_by_keyword(keyword):
    # Escape the keyword to prevent regex injection attacks
    escaped_keyword = re.escape(keyword)

    # Debugging output
    print(f"Searching for articles with keyword: {escaped_keyword}")

    try:
        # Perform a case-insensitive search in the 'title' field
        articles = collection.find({
            'title': {'$regex': escaped_keyword, '$options': 'i'}
        })

        # Collect the titles of the articles that match the keyword
        result = [article.get('title', 'No Title') for article in articles]

        # Debugging output
        print(f"Found articles: {result}")

        return jsonify(result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": "An error occurred while fetching articles."}), 500


@app.route('/articles_by_author/<author_name>', methods=['GET'])
def articles_by_author(author_name):
    try:
        # Perform a case-insensitive search in the 'author' field
        articles = collection.find({
            'author': {'$regex': f'^{author_name}$', '$options': 'i'}
        })

        # Collect the titles of the articles written by the specified author
        result = [article.get('title', 'No Title') for article in articles]

        # Check if no articles are found
        if not result:
            return jsonify({"message": "No articles found for the specified author."}), 404

        return jsonify(result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": "An error occurred while fetching articles."}), 500


@app.route('/top_classes', methods=['GET'])
def top_classes():
    try:
        # Define the aggregation pipeline
        pipeline = [
            {"$unwind": "$classes"},  # Flatten the 'classes' array if it's an array
            {"$group": {
                "_id": "$classes",  # Group by each class
                "count": {"$sum": 1}  # Count occurrences
            }},
            {"$sort": {"count": -1}},  # Sort by count in descending order
            {"$limit": 10}  # Limit to top 10 results
        ]

        # Perform aggregation
        results = collection.aggregate(pipeline)

        # Format the result
        result = {}
        for item in results:
            # Ensure '_id' is a string and use it as the key
            class_name = str(item['_id'])
            result[class_name] = f"({item['count']} articles)"

        # Check if no classes are found
        if not result:
            return jsonify({"message": "No classes found."}), 404

        return jsonify(result)

    except Exception as e:
        # Print the exception with a more detailed error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching top classes: {e}"}), 500


@app.route('/articles_with_video', methods=['GET'])
def articles_with_video():
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Query the database for articles where video_duration is not null
        articles = collection.find({
            'video_duration': {'$ne': None}  # $ne means "not equal to"
        })

        # Collect the titles of the articles that contain a video
        result = [article.get('title', 'No Title') for article in articles]

        # Check if no articles are found
        if not result:
            return jsonify({"message": "No articles with video found."}), 404

        return jsonify(result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles with video: {e}"}), 500

@app.route('/article_details/<postid>', methods=['GET'])
def article_details(postid):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Debug: Print the postid being queried
        print(f"Querying for postid: {postid}")

        # Find the article by postid
        article = collection.find_one({'postid': postid})

        # Debug: Print the result of the query
        if article is None:
            print("No article found.")
            return jsonify({"error": "Article not found."}), 404

        print(f"Article found: {article}")

        # Extract relevant details
        details = {
            "URL": article.get('url', 'No URL'),
            "Title": article.get('title', 'No Title'),
            "Keywords": article.get('keywords', [])
        }

        return jsonify(details)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching article details: {e}"}), 500


@app.route('/articles_by_year/<int:year>', methods=['GET'])
def articles_by_year(year):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Validate year
        if year < 1900 or year > datetime.now().year:
            return jsonify({"error": "Year out of range."}), 400

        # Create start and end dates for the given year
        start_date = datetime(year, 1, 1)
        end_date = datetime(year + 1, 1, 1)

        # Debug: Print the start and end dates
        print(f"Querying for year: {year}")
        print(f"Start Date: {start_date.isoformat()}")
        print(f"End Date: {end_date.isoformat()}")

        # Query the database for articles published in the given year
        count = collection.count_documents({
            'published_time': {
                '$gte': start_date,
                '$lt': end_date
            }
        })

        # Debug: Print the count result
        print(f"Count of articles: {count}")

        # Format the result
        result = {
            "year": year,
            "count": count
        }

        return jsonify(result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles by year: {e}"}), 500


@app.route('/longest_articles', methods=['GET'])
def longest_articles():
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Aggregate query to get the top 10 articles by word count
        pipeline = [
            {"$sort": {"word_count": -1}},  # Sort by word_count in descending order
            {"$group": {
                "_id": "$title",  # Group by title to ensure uniqueness
                "word_count": {"$first": "$word_count"}  # Get the word_count of the first document in each group
            }},
            {"$sort": {"word_count": -1}},  # Sort again after grouping
            {"$limit": 10},  # Limit to the top 10 articles
            {"$project": {"_id": 0, "title": "$_id", "word_count": 1}}  # Format the output
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Format the result
        formatted_result = [
            {"title": article.get('title', 'No Title'), "word_count": article.get('word_count', 0)}
            for article in result
        ]

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching longest articles: {e}"}), 500


@app.route('/shortest_articles', methods=['GET'])
def shortest_articles():
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Aggregate query to get the top 10 articles by lowest word count
        pipeline = [
            {"$sort": {"word_count": 1}},  # Sort by word_count in ascending order
            {"$limit": 10},  # Limit to the top 10 articles
            {"$project": {"title": 1, "word_count": 1}}  # Project only title and word_count
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Format the result
        formatted_result = [
            {"title": article.get('title', 'No Title'), "word_count": article.get('word_count', 0)}
            for article in result
        ]

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching shortest articles: {e}"}), 500


@app.route('/articles_by_keyword_count', methods=['GET'])
def articles_by_keyword_count():
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Aggregate query to group articles by the number of keywords and count them
        pipeline = [
            {"$project": {"keyword_count": {"$size": "$keywords"}}},  # Count the number of keywords
            {"$group": {
                "_id": "$keyword_count",  # Group by the number of keywords
                "count": {"$sum": 1}  # Count the number of articles for each keyword count
            }},
            {"$sort": {"_id": 1}}  # Sort by the number of keywords (ascending order)
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Format the result
        formatted_result = [
            {"keyword_count": article.get('_id', 0), "article_count": article.get('count', 0)}
            for article in result
        ]

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles by keyword count: {e}"}), 500


@app.route('/articles_with_thumbnail', methods=['GET'])
def articles_with_thumbnail():
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Query to find articles with a non-null thumbnail
        query = {"thumbnail": {"$exists": True, "$ne": None}}

        # Find articles matching the query
        articles = collection.find(query, {"title": 1, "_id": 0})  # Project only the title

        # Format the result
        formatted_result = [article.get('title', 'No Title') for article in articles]

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles with thumbnail: {e}"}), 500


@app.route('/articles_updated_after_publication', methods=['GET'])
def articles_updated_after_publication():
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Query to find articles where last_updated is after published_time
        query = {
            "$expr": {
                "$gt": ["$last_updated", "$published_time"]  # Check if last_updated > published_time
            }
        }

        # Find articles matching the query
        articles = collection.find(query, {"title": 1, "_id": 0})  # Project only the title

        # Format the result
        formatted_result = [article.get('title', 'No Title') for article in articles]

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles updated after publication: {e}"}), 500


@app.route('/articles_by_coverage/<coverage>', methods=['GET'])
def articles_by_coverage(coverage):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Print the coverage parameter for debugging
        print(f"Coverage parameter: {coverage}")

        # Query to find articles where coverage is in the classes field
        # Handle cases where 'classes' is an array or a single value
        query = {"classes": {"$in": [coverage]}}

        # Print the query for debugging
        print(f"Query: {query}")

        # Find articles matching the query
        articles = collection.find(query, {"title": 1, "_id": 0})

        # Convert articles to a list and format the result
        formatted_result = [article.get('title', 'No Title') for article in articles]

        # Print the result for debugging
        print(f"Result: {formatted_result}")

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles by coverage: {e}"}), 500


@app.route('/popular_keywords_last_X_days/<int:days>', methods=['GET'])
def popular_keywords_last_X_days(days):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Calculate the date range for the last X days
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        # Print the date range for debugging
        print(f"Date Range: Start Date - {start_date}, End Date - {end_date}")

        # Query to find articles published in the last X days
        pipeline = [
            {"$match": {
                "published_time": {"$gte": start_date, "$lte": end_date},
                "keywords": {"$exists": True}
            }},
            {"$unwind": "$keywords"},
            {"$group": {
                "_id": "$keywords",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]

        # Print the aggregation pipeline for debugging
        print(f"Aggregation Pipeline: {pipeline}")

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Print the result of the aggregation
        print(f"Aggregation Result: {result}")

        # Format the result
        formatted_result = [
            {keyword["_id"]: keyword["count"]} for keyword in result
        ]

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching popular keywords: {e}"}), 500


@app.route('/articles_by_month/<int:year>/<int:month>', methods=['GET'])
def articles_by_month(year, month):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Construct the start and end dates for the given month and year
        start_date = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        next_month = month % 12 + 1
        next_year = year + (month // 12)
        end_date = datetime(next_year, next_month, 1, 0, 0, 0, tzinfo=timezone.utc) - timedelta(seconds=1)

        # Debug: Print the date range
        print(f"Date Range: Start Date - {start_date.isoformat()}, End Date - {end_date.isoformat()}")

        # Query to find articles published in the specified month and year
        pipeline = [
            {"$match": {
                "published_time": {"$gte": start_date, "$lte": end_date}
            }},
            {"$count": "article_count"}
        ]

        # Debug: Print the aggregation pipeline
        print(f"Aggregation Pipeline: {pipeline}")

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Debug: Print the aggregation result
        print(f"Aggregation Result: {result}")

        # Extract count from the result
        count = result[0]["article_count"] if result else 0

        # Map month number to month name
        month_name = datetime(year, month, 1).strftime('%B')

        # Format the result
        formatted_result = {
            f"{month_name} {year}": f"({count} articles)"
        }

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles by month: {e}"}), 500


@app.route('/articles_by_word_count_range/<int:min_word_count>/<int:max_word_count>', methods=['GET'])
def articles_by_word_count_range(min_word_count, max_word_count):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Query to find articles with word count in the specified range
        pipeline = [
            {"$match": {
                "word_count": {"$gte": min_word_count, "$lte": max_word_count}
            }},
            {"$count": "article_count"}
        ]

        # Print the aggregation pipeline for debugging
        print(f"Aggregation Pipeline: {pipeline}")

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Print the result of the aggregation
        print(f"Aggregation Result: {result}")

        # Extract count from the result
        count = result[0]["article_count"] if result else 0

        # Format the result
        formatted_result = {
            f"Articles between {min_word_count} and {max_word_count} words": f"({count} articles)"
        }

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles by word count range: {e}"}), 500


@app.route('/articles_with_specific_keyword_count/<int:count>', methods=['GET'])
def articles_with_specific_keyword_count(count):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Query to find articles with exactly 'count' number of keywords
        pipeline = [
            {"$match": {
                "keywords": {"$size": count}
            }},
            {"$count": "article_count"}
        ]

        # Print the aggregation pipeline for debugging
        print(f"Aggregation Pipeline: {pipeline}")

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Print the result of the aggregation
        print(f"Aggregation Result: {result}")

        # Extract count from the result
        article_count = result[0]["article_count"] if result else 0

        # Format the result
        formatted_result = {
            f"Articles with exactly {count} keywords": f"({article_count} articles)"
        }

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles with specific keyword count: {e}"}), 500


@app.route('/articles_by_specific_date/<date>', methods=['GET'])
def articles_by_specific_date(date):
    if collection is None:
        return jsonify({"error": "MongoDB connection error."}), 500

    try:
        # Convert date from string to datetime object
        date_object = datetime.strptime(date, '%Y-%m-%d')

        # Query to find articles published on the specified date
        pipeline = [
            {"$match": {
                "published_time": {
                    "$gte": date_object,
                    "$lt": date_object + timedelta(days=1)
                }
            }},
            {"$count": "article_count"}
        ]

        # Print the aggregation pipeline for debugging
        print(f"Aggregation Pipeline: {pipeline}")

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Print the result of the aggregation
        print(f"Aggregation Result: {result}")

        # Extract count from the result
        article_count = result[0]["article_count"] if result else 0

        # Format the result
        formatted_result = {
            f"Articles published on {date}": f"({article_count} articles)"
        }

        return jsonify(formatted_result)

    except Exception as e:
        # Print the exception and return a 500 status code with error message
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred while fetching articles by specific date: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True)