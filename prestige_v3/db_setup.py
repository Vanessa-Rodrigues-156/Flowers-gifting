from database import Database

def create_database(db_path: str = 'prestige_flowers_v3.db'):
    db = Database(db_path)
    db.init_database()
    print(f"Database initialized: {db_path}")

if __name__ == '__main__':
    create_database()