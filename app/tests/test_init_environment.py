from app.pipeline.init_environment import init_environment
from app.config.settings import SQLITE_DB_PATH

def main():
    init_environment()

    print("Init environment test completed.")
    print("DB exists: ", SQLITE_DB_PATH.exists())

if __name__ == "__main__":
    main()