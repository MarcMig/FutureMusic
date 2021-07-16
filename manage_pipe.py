import json
import os
from Naked.toolshed.shell import execute_js


with open('track_urls_all_v1.csv.json') as j:
    json_file = json.loads(j.read())

i = 0
tempfile = {}

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
filename = __location__ + "/Lambda/Lambda/trackurls.json"
js_path = __location__+ "/Lambda/Lambda//index.js"
print(filename)

for index,item in json_file.items():
    tempfile[index] = item
    i += 1
    if i % 5 == 0 or i == len(json_file):
        with open(filename, 'w') as outputfile:
            if i == len(json_file):
                tempfile[index] = item
            json.dump(tempfile, outputfile)
            tempfile = {}
        response = execute_js(js_path)


