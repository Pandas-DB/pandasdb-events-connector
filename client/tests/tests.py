from os import getenv
import pandas as pd
import time

from pandasdb_events_connector.client import EventsClient


def get_reference_dataset():
    # Test small dataframe with date partitioning
    return pd.DataFrame({
        'transaction_date': pd.date_range(start='2024-01-01', periods=5),
        'user_id': range(1000, 1005),
        'amount': [100, 200, 300, 400, 500],
        'category': ['A', 'B', 'A', 'C', 'B']
    })


def test_post_dataframe(client):
    print('... Upload small dataframe')
    small_df = get_reference_dataset()
    start_time = time.time()
    result = client.post_dataframe(
        df=small_df,
        dataframe_name='testZ/small-file',
        chunk_size=5 * 1024 * 1024
    )
    print(f"Uploaded {len(small_df)} rows | "
          f"In: {round(time.time() - start_time)} seconds | "
          f"Avg performance: {round(len(small_df) / (time.time() - start_time))} rows uploaded per second")
    assert result.get('key')


def main():
    # Initialize client
    client = EventsClient(
        api_url=getenv('API_URL'),
        user=getenv('USER'),
        password=getenv('PASS')
    )

    try:
        print("Testing posting data...")
        test_post_dataframe(client)
    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()
