from google_play_scraper import app, Sort, reviews_all
from app_store_scraper import AppStore
from app_store_scraper import AppStore
import pandas as pd
import numpy as np
import json, os, uuid
import time

def scrape_google_play_reviews(app_id, countries, languages, delay=5):
    all_reviews = []

    for country in countries:
        for lang in languages:
            try:
                print(f"Fetching reviews for country: {country}, language: {lang}")
                reviews = reviews_all(
                    app_id,
                    lang=lang,
                    country=country,
                    sort=Sort.NEWEST,
                )
                for review in reviews:
                    review['country_code'] = country
                    review['language_code'] = lang
                all_reviews.extend(reviews)
                time.sleep(delay)  # Rate limiting
            except Exception as e:
                print(f"Error fetching reviews for {country}-{lang}: {e}")
    
    g_df = pd.DataFrame(all_reviews)
    g_df.drop(columns=['userImage', 'reviewCreatedVersion'], inplace=True, errors='ignore')
    g_df.rename(columns={
        'score': 'rating',
        'userName': 'user_name',
        'reviewId': 'review_id',
        'content': 'review_description',
        'at': 'review_date',
        'replyContent': 'developer_response',
        'repliedAt': 'developer_response_date',
        'thumbsUpCount': 'thumbs_up',
    }, inplace=True)
    g_df.insert(loc=0, column='source', value='Google Play')
    g_df['review_title'] = None
    return g_df

def scrape_app_store_reviews(app_name, app_id, countries, delay=5):
    all_reviews = []

    for country in countries:
        try:
            print(f"Fetching reviews for country: {country}")
            app = AppStore(country=country, app_name=app_name, app_id=app_id)
            app.review()
            reviews = app.reviews
            for review in reviews:
                review['country_code'] = country
                review['review_id'] = str(uuid.uuid4())  # Generate unique ID
            all_reviews.extend(reviews)
            time.sleep(delay)  # Rate limiting
        except Exception as e:
            print(f"Error fetching reviews for {country}: {e}")
    
    a_df = pd.DataFrame(all_reviews)
    a_df.rename(columns={
        'review': 'review_description',
        'userName': 'user_name',
        'date': 'review_date',
        'title': 'review_title',
        'developerResponse': 'developer_response',
    }, inplace=True)
    a_df['source'] = 'App Store'
    a_df['developer_response_date'] = None
    a_df['thumbs_up'] = None
    a_df['language_code'] = 'en'
    return a_df
