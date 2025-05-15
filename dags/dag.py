#tasks : 
    # 1) fetch amazon data (extract) 
    # 2) clean data (transform) 
    # 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def get_amazon_data_books(num_books, ti):
    import logging
    logging.info(f"Starting Amazon book data collection for {num_books} books")
    
    # Base URL of the Amazon search results for data science books
    base_url = f"https://www.amazon.com/s?k=data+science+books"

    books = []
    seen_titles = set()  # To keep track of seen titles

    page = 1
    
    logging.info(f"Headers being used: {headers}")

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        logging.info(f"Scraping page {page} from URL: {url}")
        
        try:
            # Send a request to the URL
            logging.info("Sending request to Amazon...")
            response = requests.get(url, headers=headers)
            logging.info(f"Response received with status code: {response.status_code}")
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the content of the request with BeautifulSoup
                logging.info("Parsing HTML content with BeautifulSoup")
                soup = BeautifulSoup(response.content, "html.parser")
                
                # Find book containers
                book_containers = soup.find_all("div", {"class": "a-section a-spacing-small puis-padding-left-small puis-padding-right-small"})
                logging.info(f"Found {len(book_containers)} result items on page {page}")
                
                books_found_on_page = 0
                
                # Loop through the book containers and extract data
                for book in book_containers:
                    title_h2 = book.find("h2")
                    title = title_h2.find("span") if title_h2 else None
                    
                    author_div = book.find("div", {"class": "a-row a-size-base a-color-secondary"})
                    author = author_div.find("a") if author_div else None
                    
                    price_count = book.find("span", {"class": "a-price"})
                    price = price_count.find("span", {"class": "a-offscreen"})
                    rating = book.find("span", {"class": "a-icon-alt"})
                    
                    # Debug what was found for each element
                    logging.info(f"Element search results - Title: {title is not None}, "
                            f"Author: {author is not None}, "
                            f"Price: {price is not None}, "
                            f"Rating: {rating is not None}")
                    
                    if title and author and price and rating:
                        book_title = title.text.strip()
                        author_text = author.text.strip()
                        price_text = price.text.strip()
                        rating_text = rating.text.strip()
                        
                        logging.info(f"Found book - Title: {book_title}, "
                                f"Author: {author_text}, "
                                f"Price: {price_text}, "
                                f"Rating: {rating_text}")
                        
                        # Check if title has been seen before
                        if book_title not in seen_titles:
                            seen_titles.add(book_title)
                            books.append({
                                "Title": book_title,
                                "Author": author_text,
                                "Price": price_text,
                                "Rating": rating_text,
                            })
                            books_found_on_page += 1
                
                logging.info(f"Added {books_found_on_page} new books from page {page}")
                logging.info(f"Total books collected so far: {len(books)}")
                
                # If no books were found on this page, we might have reached the end
                if books_found_on_page == 0:
                    logging.warning(f"No new books found on page {page}, might be at the end of results")
                    if page > 3:  # After a few pages with no results, stop
                        break
                
                # Increment the page number for the next iteration
                page += 1
            else:
                logging.error(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                break
        except Exception as e:
            logging.error(f"Exception occurred while processing page {page}: {str(e)}")
            break

    # Limit to the requested number of books
    if len(books) > num_books:
        logging.info(f"Limiting results from {len(books)} to requested {num_books} books")
        books = books[:num_books]
    
    logging.info(f"Final number of books before creating DataFrame: {len(books)}")
    
    # Check if we have any books
    if not books:
        logging.error("No books were collected! Check the Amazon selectors or request headers.")
        # Create an empty DataFrame to avoid errors later
        df = pd.DataFrame(columns=["Title", "Author", "Price", "Rating"])
    else:
        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(books)
        
        # Remove duplicates based on 'Title' column
        original_count = len(df)
        df.drop_duplicates(subset="Title", inplace=True)
        final_count = len(df)
        logging.info(f"Removed {original_count - final_count} duplicate entries")
    
    # Log DataFrame summary
    logging.info(f"Final DataFrame shape: {df.shape}")
    logging.info(f"DataFrame columns: {df.columns.tolist()}")
    logging.info(f"DataFrame sample:\n{df.head(3) if not df.empty else 'Empty DataFrame'}")
    
    # Push the DataFrame to XCom
    logging.info("Pushing data to XCom using CSV format")
    
    try:
        csv_data = df.to_csv(index=False)
        logging.info(f"CSV data size: {len(csv_data)} characters")
        
        # Push to XCom
        ti.xcom_push(key='book_data', value=csv_data)
        logging.info("Successfully pushed data to XCom")
        
        # Verification step - try to pull immediately to check
        try:
            test_pull = ti.xcom_pull(key='book_data')
            if test_pull:
                logging.info(f"Verification successful: XCom data retrieved, size: {len(test_pull)}")
            else:
                logging.warning("Verification failed: Could not pull data back from XCom!")
        except Exception as e:
            logging.error(f"Error during verification pull: {str(e)}")
            
    except Exception as e:
        logging.error(f"Error pushing data to XCom: {str(e)}")
        raise

    return f"Collected {len(df)} books"
    
    # Push the DataFrame to XCom
    # ti.xcom_push(key='book_data', value=df.to_dict('records'))

    ti.xcom_push(key='book_data', value=df.to_csv())

def insert_book_data_into_postgres(ti):
    try:
        csv_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
        
        if not csv_data:
            raise ValueError("No book data found")

        # Convert CSV string back to DataFrame
        import io
        df = pd.read_csv(io.StringIO(csv_data))
        
        print(f"Retrieved {len(df)} books from XCom")
        
        postgres_hook = PostgresHook(postgres_conn_id='books_connection')
        
        # Insert each row into Postgres
        for _, row in df.iterrows():
            postgres_hook.run(
                """
                INSERT INTO books (title, authors, price, rating)
                VALUES (%s, %s, %s, %s)
                """,
                parameters=(
                    row['Title'],
                    row['Author'],
                    row['Price'],
                    row['Rating']
                )
            )
        
        print("All books inserted successfully")
    
    except Exception as e:
        print(f"Error in insert_book_data_into_postgres: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgresdb',
    schedule=timedelta(days=1),
)

fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

# dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task