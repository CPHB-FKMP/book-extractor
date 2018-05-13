import tarfile, os, zipfile, re, pprint, csv, glob, shutil
import pandas as pd
import regex
from concurrent.futures import ProcessPoolExecutor

pp = pprint.PrettyPrinter(indent=2)

def main():
    print("Main method called")

    # files = glob.glob('root/zipfiles/*')
    # for f in files:
    #     os.remove(f)
    #
    # shutil.rmtree('data/books')
    # if not os.path.exists('data/books'):
    #     os.makedirs('data/books')
    #
    # tar = tarfile.open(name="root/archive.tar")
    # tar.extractall()
    #
    # dir_name = "root/zipfiles"
    # extension = ".zip"
    #
    # for item in os.listdir(dir_name):
    #     if item.endswith(extension):
    #         file_name = dir_name + "/" + item
    #         zip_ref = zipfile.ZipFile(file_name) # create zipfile object
    #         try:
    #             zip_ref.extractall("data/books") # extract file to dir
    #             print(file_name)
    #         except NotImplementedError:
    #             print("Could not unzip: " + file_name + " - continuing")
    #         zip_ref.close()

    dir_name = "data/books"
    extension = ".txt"

    cities_csv = pd.read_csv('data/cities/cities15000.csv', header=0, sep=';', usecols=['englishName', 'latitude', 'longitude'])

    books = list()
    authors = set()
    complete_author_list = list()
    cities = set()

    th_ex = ProcessPoolExecutor(max_workers=4)
    futures = []

    count = 0
    for root, directories, files in os.walk(dir_name):
        for file in files:
            if file.endswith(extension):
                count = count + 1
                future = th_ex.submit(get_results, root, file, cities_csv, count)
                futures.append(future)
                pp.pprint(count)

    th_ex.shutdown(wait=True)
    print("Done with everything. Unwrapping results...")

    for th in futures:
        result = th.result()
        books.append(result[0])
        exit(1)
        for author in result[1]:
            authors.add(author)

        for city in result[2]:
            cities.add(city)

    for idx, val in enumerate(authors):
        complete_author_list.append((idx, val))
    with open('data/csv/cities.csv', 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        writer.writerow(['name', 'latitude', 'longitude'])
        for city in cities:
            writer.writerow(city)

    with open('data/csv/books.csv', 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, quotechar='')
        writer.writerow(['id', 'title'])
        for value in books:
            title = value['title']
            title = title.rstrip()
            title = " ".join(title.split())
            writer.writerow([value['id'], title])

    with open('data/csv/authors.csv', 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, quotechar='')
        writer.writerow(['id', 'name'])
        for author in complete_author_list:
            auth = " ".join(author[1].split())
            if auth or auth is not None:
                writer.writerow([author[0], auth])

    with open('data/csv/books-cities.csv', 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        writer.writerow(['book_id', 'latitude', 'longitude'])

        for book in books:
            for city in book['cities']:
                writer.writerow([book['id'], city[1], city[2]])

    with open('data/csv/books-authors.csv', 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        writer.writerow(['book_id', 'author_id'])
        for book in books:
            for author in book['authors']:
                auth = [item for item in complete_author_list if item[1] == author]
                writer.writerow([book['id'], auth[0][0]])


def get_results(root, file, cities_csv, count):
    book = dict()
    authors = set()
    cities = set()
    full_path = os.path.join(root, file)
    with open(full_path, 'rt', encoding='ISO-8859-1') as txt_file:
        file_data = txt_file.read()
        file_name = txt_file.name.split('/')

        book_data = file_data
        if '***' in file_data:
            book_data = "".join(file_data.split('***')[1:])
        id = file_name[len(file_name) - 1].split('.')[0]
        book['id'] = id
        book['cities'] = set()
        book['authors'] = list()
        temp_title = regex.search(r"(?<=Title:\s)([^&]*?)(?:(?:\r*\n){2})", file_data)
        if temp_title is None:
            book['title'] = "Unknown title"
        else:
            book['title'] = temp_title.group()
        authors_str = ",".join(regex.findall(r"(?<=^Author:\s)([^&]*?)(?:(?:\r*\n){2})", file_data, flags=re.MULTILINE))
        re.sub(r"\band\b", "", authors_str)
        authors_list = authors_str.split(",")

        for idx, val in enumerate(authors_list):
            if not val or val is None:
                authors_list[idx] = "Unknown"

        book['authors'] = authors_list

        for auth in authors_list:
            authors.add(auth)

        cities_in_book = regex.findall(r"\p{Lu}\p{L}*(?:[\ ?-]\p{Lu}\p{L}*)*", book_data)
        cities_set = set(cities_in_book)
        try:
            cities_set.remove("I")
            cities_set.remove("The")
        except KeyError:
            print("Not found in set - skipping")

        for city in cities_set:
            if city in cities_csv['englishName'].tolist():
                rows = cities_csv[cities_csv['englishName'] == city]
                for index, row in rows.iterrows():
                    book['cities'].add((city, row['latitude'], row['longitude']))

                    cities.add((city, row['latitude'], row['longitude']))

        print("Done with #%d" % count)
        return book, authors, cities


if __name__ == "__main__":
    main()