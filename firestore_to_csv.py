#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  6 21:00:18 2024

@author: xiawang
"""

import firebase_admin
from firebase_admin import credentials, firestore
import csv

def export_firestore_to_csv(cred_path, collection_path, output_file):
    """
    Export data from a specified Firestore collection to a CSV file.

    :param cred_path: Path to the Firebase credentials JSON file.
    :param collection_path: Path to the Firestore collection to export.
    :param output_file: Name of the output CSV file.
    """
    print("Data export starts")

    # Initialize Firestore with the provided credentials
    cred = credentials.Certificate(cred_path)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    # Get all users in study
    users = db.collection(collection_path).stream()
    
    # Get id's of all users
    user_ids = []
    for user in users:
        user_ids.append(user.id)
    
    for user_id in user_ids:
        user_responses = db.collection(collection_path).document(user_id).collection("responses").stream()
        
        # Prepare CSV file
        with open(user_id + ".csv", 'w', newline='') as file:
            
            # Identify all unique user names
            fieldnames = set()
            for user_response in user_responses:
                fieldnames.update(user_response.to_dict().keys())
        
            # Sort field names for consistent column order
            fieldnames = sorted(fieldnames)
            
            # Re-fetch documents for writing to CSV
            user_responses = db.collection(collection_path).document(user_id).collection("responses").stream()
            
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
    
            for user_response in user_responses:
                # Write document data to CSV
                writer.writerow(user_response.to_dict())

    print("Data export complete.")

# Example usage
export_firestore_to_csv('smarter-time-use-surveys-firebase-adminsdk-g1olm-65757b9d37.json', 'study1', 'output.csv')
