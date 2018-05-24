import tarfile, os, zipfile, re, pprint, csv, glob
import pandas as pd


def main():
    with open('/Users/Mikkel/Desktop/full-csv/csv/postgres-cities.csv', 'r', newline='', encoding='utf-8') as old_csv:
        with open('/Users/Mikkel/Desktop/full-csv/csv/postgres-cities-corrected.csv', 'w+', newline='', encoding='utf-8') as new_csv:
            reader = csv.reader(old_csv, delimiter=';', escapechar='\\')
            writer = csv.writer(new_csv, delimiter='|', escapechar='\\')
            writer.writerow(['name', 'location', 'latitude', 'longitude'])

            next(reader)
            for row in reader:
                lat_long = row[1].split(',')
                writer.writerow([row[0], row[1], lat_long[0], lat_long[-1]])


if __name__ == "__main__":
    main()