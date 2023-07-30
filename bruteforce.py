import csv
import sys
from tqdm import tqdm

follows = dict()
friendOf = dict()

def read_csv_file(file_path, table):
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if len(row) == 2:
                if row[0] not in table:
                    table[row[0]] = dict()
                table[row[0]][row[1]] = True
            else:
                print("unexpected line", row)
                sys.exit(1)


followsFile = './preprocessed-small-watdiv/follows.txt'
friendOfFile = './preprocessed-small-watdiv/friendOf.txt'

read_csv_file(followsFile, follows)
read_csv_file(friendOfFile, friendOf)

ans = 0

# ln = 0
# for user in follows:
#     ln += len(follows[user])
# print(ln)

a = tqdm(range(0, 1000))
for user1 in a:
    a.set_description(f"ans = {ans}")
    for user2 in range(0, 1000):
        for user3 in range(0, 1000):
            suser1 = f"wsdbm:User{user1}"
            suser2 = f"wsdbm:User{user2}"
            suser3 = f"wsdbm:User{user3}"
            if suser1 in follows and suser2 in friendOf and suser2 in follows[suser1] and suser3 in friendOf[suser2]:
                ans += 1

print(ans)
