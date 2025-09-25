"""Interactive database configuration script"""
import os
import getpass

def setup_database_config():
    """Interactive setup for database configuration"""
    print("🔧 Database Configuration Setup")
    print("=" * 40)
    
    # Get current .env values
    env_path = ".env"
    
    print("Please provide your MySQL database information:")
    print("(Press Enter to use default values shown in brackets)")
    print()
    
    # Get user input
    host = input("MySQL Host [localhost]: ").strip() or "localhost"
    port = input("MySQL Port [3306]: ").strip() or "3306"
    username = input("MySQL Username [root]: ").strip() or "root"
    password = getpass.getpass("MySQL Password: ").strip()
    database = input("Database Name [log_management]: ").strip() or "log_management"
    
    # Construct DATABASE_URL
    database_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    
    print("\n📝 Configuration Summary:")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Username: {username}")
    print(f"Password: {'*' * len(password)}")
    print(f"Database: {database}")
    print(f"URL: mysql+pymysql://{username}:{'*' * len(password)}@{host}:{port}/{database}")
    
    confirm = input("\nIs this correct? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Configuration cancelled.")
        return False
    
    # Read current .env file
    env_lines = []
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            env_lines = f.readlines()
    
    # Update relevant lines
    updated_lines = []
    database_url_updated = False
    db_host_updated = False
    db_port_updated = False
    db_user_updated = False
    db_password_updated = False
    db_name_updated = False
    
    for line in env_lines:
        if line.startswith('DATABASE_URL='):
            updated_lines.append(f'DATABASE_URL={database_url}\n')
            database_url_updated = True
        elif line.startswith('DB_HOST='):
            updated_lines.append(f'DB_HOST={host}\n')
            db_host_updated = True
        elif line.startswith('DB_PORT='):
            updated_lines.append(f'DB_PORT={port}\n')
            db_port_updated = True
        elif line.startswith('DB_USER='):
            updated_lines.append(f'DB_USER={username}\n')
            db_user_updated = True
        elif line.startswith('DB_PASSWORD='):
            updated_lines.append(f'DB_PASSWORD={password}\n')
            db_password_updated = True
        elif line.startswith('DB_NAME='):
            updated_lines.append(f'DB_NAME={database}\n')
            db_name_updated = True
        else:
            updated_lines.append(line)
    
    # Add missing entries
    if not database_url_updated:
        updated_lines.append(f'DATABASE_URL={database_url}\n')
    if not db_host_updated:
        updated_lines.append(f'DB_HOST={host}\n')
    if not db_port_updated:
        updated_lines.append(f'DB_PORT={port}\n')
    if not db_user_updated:
        updated_lines.append(f'DB_USER={username}\n')
    if not db_password_updated:
        updated_lines.append(f'DB_PASSWORD={password}\n')
    if not db_name_updated:
        updated_lines.append(f'DB_NAME={database}\n')
    
    # Write updated .env file
    with open(env_path, 'w') as f:
        f.writelines(updated_lines)
    
    print(f"✅ Configuration saved to {env_path}")
    return True

if __name__ == "__main__":
    if setup_database_config():
        print("\n🧪 Testing connection...")
        os.system("python test_db_connection.py")