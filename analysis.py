# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 16:57:59 2024

@author: Vineet Pillai
"""

from elasticsearch import Elasticsearch

# Elasticsearch configuration
es = Elasticsearch("http://localhost:9200")

# Function to perform analysis on the stored data
def perform_analysis():
    # Example: Correlation between clicks and conversions
    # Aggregation to calculate the sum of clicks and conversions for each user
    aggregation = {
        "size": 0,
        "aggs": {
            "clicks_sum": {"sum": {"field": "clicks"}},
            "conversions_sum": {"sum": {"field": "conversions"}},
            "clicks_stats": {"stats": {"field": "clicks"}},
            "conversions_stats": {"stats": {"field": "conversions"}}
        }
    }
    # Perform search with aggregation
    result = es.search(index='clicks_conversions_index', body={"query": {"match_all": {}}, "aggs": aggregation})
    # Extract aggregated data
    clicks_sum = result['aggregations']['clicks_sum']['value']
    conversions_sum = result['aggregations']['conversions_sum']['value']
    clicks_stats = result['aggregations']['clicks_stats']
    conversions_stats = result['aggregations']['conversions_stats']
    # Calculate correlation
    correlation = clicks_sum / conversions_sum if conversions_sum != 0 else None
    # Extract statistical metrics
    clicks_mean = clicks_stats['avg']
    clicks_median = clicks_stats['median']
    clicks_std_dev = clicks_stats['std_deviation']
    conversions_mean = conversions_stats['avg']
    conversions_median = conversions_stats['median']
    conversions_std_dev = conversions_stats['std_deviation']
    # Print analysis results
    print("Correlation between clicks and conversions:", correlation)
    print("Clicks Mean:", clicks_mean)
    print("Clicks Median:", clicks_median)
    print("Clicks Standard Deviation:", clicks_std_dev)
    print("Conversions Mean:", conversions_mean)
    print("Conversions Median:", conversions_median)
    print("Conversions Standard Deviation:", conversions_std_dev)

# Call the function to perform analysis
perform_analysis()
