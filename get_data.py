import csv

with open('data/ratings.csv', 'r') as infile:
    with open('data/new_ratings.csv', 'w') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        count = 0

        for row in reader:
            if count == 0:
                writer.writerow(row)
                count += 1
            elif int(row[1]) < 1000:
                writer.writerow(row)

