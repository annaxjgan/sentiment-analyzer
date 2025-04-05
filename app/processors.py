from collections import defaultdict
from utils import parse_date

def process_chunk(chunk):
    """
    Process a chunk of records and return sentiment aggregations
    :param chunk: List of records
    :return: Tuple of dictionaries containing hour and user sentiment scores
    """
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

def display_results(final_time_score_data, final_user_score_data):
    """
    Display the top and bottom sentiment scores
    :param final_time_score_data: Dictionary of time-based sentiment scores
    :param final_user_score_data: Dictionary of user-based sentiment scores
    """
    import heapq

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
        account_name, account_id = item[0]
        value = item[1]
        print(f'Account id: {account_id}, Username: {account_name}, Total sentiment score: {value}')

    # Print saddest people
    print('\n=======Top 5 saddest people=========')
    for item in saddest_users:
        account_name, account_id = item[0]
        value = item[1]
        print(f'Account id: {account_id}, Username: {account_name}, Total sentiment score: {value}')