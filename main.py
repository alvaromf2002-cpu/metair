import argparse
from functions import *

def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline on aircraft JSON data.")
    parser.add_argument("--json", type=str, default="input.json", help="Path to input JSON file")
    args = parser.parse_args()

    summary = run_pipeline(args.json)
    print("ETL Pipeline completed.")
    print(f"Total records: {summary['total']}")
    print(f"Used records: {summary['used']}")
    print(f"Percentage used: {summary['used']/summary['total']*100 if summary['total'] else 0:.2f}%")
    print(f"SQL DB: {summary['db_path']}, NoSQL DB: {summary['nosql_path']}")

if __name__ == "__main__":
    main()
