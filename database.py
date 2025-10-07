import sqlite3
import pandas as pd
from datetime import datetime
import os

class ProjectDatabase:
    def __init__(self, db_path='projects.db'):
        # Check if we should use PostgreSQL
        self.db_url = os.getenv('DATABASE_URL')
        
        if self.db_url:
            # Use PostgreSQL
            import psycopg2
            self.conn = psycopg2.connect(self.db_url)
            self.is_postgres = True
        else:
            # Use SQLite (local development)
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.is_postgres = False
            
        self.init_database()
    
    def init_database(self):
        """Create the projects table if it doesn't exist"""
        cursor = self.conn.cursor()
        
        if self.is_postgres:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id SERIAL PRIMARY KEY,
                    project_name TEXT,
                    customer TEXT,
                    general_contractor TEXT,
                    announcement_date TEXT,
                    project_value TEXT,
                    jobs_created TEXT,
                    city TEXT,
                    county TEXT,
                    state TEXT,
                    article_url TEXT UNIQUE,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        else:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_name TEXT,
                    customer TEXT,
                    general_contractor TEXT,
                    announcement_date TEXT,
                    project_value TEXT,
                    jobs_created TEXT,
                    city TEXT,
                    county TEXT,
                    state TEXT,
                    article_url TEXT UNIQUE,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        self.conn.commit()
    
    def import_from_csv(self, csv_path):
        """Import projects from CSV file"""
        df = pd.read_csv(csv_path)
        
        # Rename columns to match database schema
        column_mapping = {
            'Project Name': 'project_name',
            'Customer': 'customer',
            'General Contractor': 'general_contractor',
            'Announcement Date': 'announcement_date',
            'Project Value': 'project_value',
            'Jobs Created': 'jobs_created',
            'City': 'city',
            'County': 'county',
            'State': 'state',
            'Article URL': 'article_url'
        }
        
        df = df.rename(columns=column_mapping)
        
        cursor = self.conn.cursor()
        
        for _, row in df.iterrows():
            try:
                if self.is_postgres:
                    cursor.execute('''
                        INSERT INTO projects (project_name, customer, general_contractor, 
                                            announcement_date, project_value, jobs_created,
                                            city, county, state, article_url, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (article_url) DO NOTHING
                    ''', (row.get('project_name'), row.get('customer'), 
                          row.get('general_contractor'), row.get('announcement_date'),
                          row.get('project_value'), row.get('jobs_created'),
                          row.get('city'), row.get('county'), row.get('state'),
                          row.get('article_url'), 'csv_import'))
                else:
                    cursor.execute('''
                        INSERT OR IGNORE INTO projects (project_name, customer, general_contractor, 
                                                       announcement_date, project_value, jobs_created,
                                                       city, county, state, article_url, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row.get('project_name'), row.get('customer'), 
                          row.get('general_contractor'), row.get('announcement_date'),
                          row.get('project_value'), row.get('jobs_created'),
                          row.get('city'), row.get('county'), row.get('state'),
                          row.get('article_url'), 'csv_import'))
            except Exception as e:
                print(f"Error importing row: {e}")
                continue
        
        self.conn.commit()
    
    def get_all_projects(self):
        """Get all projects as a pandas DataFrame"""
        query = "SELECT * FROM projects ORDER BY created_at DESC"
        df = pd.read_sql_query(query, self.conn)
        return df
    
    def export_to_csv(self, output_path):
        """Export all projects to CSV"""
        df = self.get_all_projects()
        df.to_csv(output_path, index=False)
        return output_path