from loader import load_all_raw_data

def main():
    print("Starting raw data load ...")
    load_all_raw_data()
    print("All raw CSV files loaded succesfully.")

if __name__ == "__main__":
    main()