import re
import sys
from tqdm import tqdm
import os

necessaryProperties = ['hasReview', 'follows', 'friendOf', 'likes']
unnecessary = "UNNECESSARY"

def parseProperty(propertyName):
    for necessaryProperty in necessaryProperties:
        if necessaryProperty in propertyName:
            return necessaryProperty
    return unnecessary

if len(sys.argv) != 2:
    print("Incorrect usage, specify `small|large`")
    sys.exit(1)

dataset = ""
dataFolder = ""
mode = sys.argv[1]

if mode == "small":
    dataFolder = "small-watdiv"
    dataset = f"./{dataFolder}/100k.txt"
elif mode == "large":
    dataFolder = "large-watdiv"
    dataset = f"./{dataFolder}/watdiv.10M.txt"
else:
    print("Incorrect usage of size argument, expected `small` or `large`")
    sys.exit(1)

resultFolder = "preprocessed-" + dataFolder

tables = dict()
for propertyName in necessaryProperties:
    tables[propertyName] = open(f'./{resultFolder}/{propertyName}.txt', 'a')

with open(dataset, 'r') as fDataset:
    pbar = tqdm(fDataset)
    skipped = 0
    processed = 0
    iline = 0
    for line in pbar:
        if iline % 100 == 0:
            pbar.set_description(f"s = {skipped}, p = {processed}")
        iline += 1

        match = re.search(r'([\S]+)\s+([\S]+)\s+("[^"]+"|([^"\s])+)\s+\.', line)

        assert match, f"The string is malformed: {line}"
        subject = match.group(1)
        property = match.group(2).strip('"')
        obj = match.group(3)

        if mode == 'large':
            subject = os.path.basename(subject.rstrip('/>'))
            obj = os.path.basename(obj.rstrip('/>'))

        key_value = f"{subject},{obj}"

        whichProperty = parseProperty(property)

        if whichProperty != unnecessary:
            processed += 1
            tables[whichProperty].write(key_value + '\n')
        else:
            skipped += 1
        # TODO close the table files, whatever
