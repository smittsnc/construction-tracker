import sqlite3
import pandas as pd
from datetime import datetime

class ProjectDatabase:
    def __init__(self, db_path='projects.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Create the projects table if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
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
        
        conn.commit()
        conn.close()
    
    def import_from_csv(self, csv_path):
        """Import projects from CSV file"""
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            print(f"❌ Error: Could not find {csv_path}")
            return
        except Exception as e:
            print(f"❌ Error reading CSV: {e}")
            return
        
        print(f"\n📊 CSV has {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}\n")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        imported_count = 0
        skipped_count = 0
        error_count = 0
        
        for idx, row in df.iterrows():
            project_name = str(row['Project Name']) if pd.notna(row['Project Name']) else 'Unknown'
            article_url = str(row['Article URL']).strip() if pd.notna(row['Article URL']) else None
            
            # Check if URL already exists
            if article_url:
                cursor.execute('SELECT project_name FROM projects WHERE article_url = ?', (article_url,))
                existing = cursor.fetchone()
                
                if existing:
                    print(f"⚠️  SKIPPED: '{project_name}' - URL already used by '{existing[0]}'")
                    skipped_count += 1
                    continue
            
            try:
                cursor.execute('''
                    INSERT INTO projects 
                    (project_name, customer, general_contractor, announcement_date, 
                     project_value, jobs_created, city, county, state, article_url, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    project_name,
                    str(row['Customer']) if pd.notna(row['Customer']) else None,
                    str(row['General Contractor']) if pd.notna(row['General Contractor']) else None,
                    str(row['Announcement Date']) if pd.notna(row['Announcement Date']) else None,
                    str(row['Project Value']) if pd.notna(row['Project Value']) else None,
                    str(row['Jobs Created']) if pd.notna(row['Jobs Created']) else None,
                    str(row['City']) if pd.notna(row['City']) else None,
                    str(row['County']) if pd.notna(row['County']) else None,
                    str(row['State']) if pd.notna(row['State']) else None,
                    article_url,
                    'Manual'
                ))
                print(f"✅ IMPORTED: {project_name}")
                imported_count += 1
                
            except sqlite3.IntegrityError as e:
                print(f"❌ ERROR: '{project_name}' - {e}")
                error_count += 1
            except Exception as e:
                print(f"❌ ERROR: '{project_name}' - {e}")
                error_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"✅ Successfully imported: {imported_count}")
        print(f"⚠️  Skipped (duplicates): {skipped_count}")
        print(f"❌ Errors: {error_count}")
        print(f"{'='*60}\n")
    
    def get_all_projects(self):
        """Get all projects as DataFrame"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM projects ORDER BY created_at DESC", conn)
        conn.close()
        return df
    
    def get_projects_by_state(self, state):
        """Get projects filtered by state"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT * FROM projects WHERE state = ? ORDER BY created_at DESC", 
            conn, 
            params=(state,)
        )
        conn.close()
        return df
    
    def export_to_csv(self, output_path='export.csv'):
        """Export all projects to CSV"""
        df = self.get_all_projects()
        df.to_csv(output_path, index=False)
        print(f"✅ Exported {len(df)} projects to {output_path}")
        return output_path
    
    def show_database_contents(self):
        """Display what's currently in the database"""
        df = self.get_all_projects()
        print(f"\n{'='*60}")
        print(f"📊 TOTAL PROJECTS IN DATABASE: {len(df)}")
        print(f"{'='*60}\n")
        
        if len(df) > 0:
            for idx, row in df.iterrows():
                print(f"{idx+1}. {row['project_name']} ({row['state']})")
        else:
            print("Database is empty!")
        print()

if __name__ == "__main__":
    print("\n🏗️  CONSTRUCTION PROJECT TRACKER - DATABASE IMPORT\n")
    
    db = ProjectDatabase()
    
    print("Importing projects from initial_projects.csv...")
    db.import_from_csv('initial_projects.csv')
    
    db.show_database_contents()