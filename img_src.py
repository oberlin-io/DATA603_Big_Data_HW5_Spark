from pyspark import SparkContext
from pyspark import SparkConf
import requests
import string
import re
import os, sys

'''
.take() and saveAsTextFile() error out:
Python in worker has different version 2.7 than that in driver 3.5,
PySpark cannot run with different minor versions.
Please check environment variables PYSPARK_PYTHON and
PYSPARK_DRIVER_PYTHON are correctly set.


Ran this in terminal
$export PYSPARK_PYTHON=python3
$export PYSPARK_DRIVER_PYTHON=python3
still didnt work
'''
#os.environ['PYSPARK_PYTHON'] = sys.executable #?
os.environ['PYSPARK_PYTHON']='python3.5'
#os.environ['PYSPARK_PYTHON']='/usr/local/lib/python3.5'
os.environ['PYSPARK_DRIVER_PYTHON']='python3.5'
#os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/lib/python3.5'

conf = SparkConf().setAppName('img src scrape').setMaster('local')
sc = SparkContext(conf=conf)
#sc = SparkContext()

def get_html_file(url):
    f_name = url
    for char in string.punctuation:
        f_name = f_name.replace(char,'')
    f_name += '.html'
    page = requests.get(url)
    page.encoding = 'utf-8' #new
    html = page.text
    with open(f_name, 'w') as f: f.write(html)
    return f_name
f_name = get_html_file('https://www.umbc.edu/')
html_lines = sc.textFile(f_name)
#html_lines.saveAsTextFile('html_lines')

img_pat = r'<img[^>]+>'
img_elements = html_lines.map(lambda x: re.findall(img_pat, x))
img_elements.saveAsTextFile('img_elements')

src_pat = r'src=[\'|"]([^\s]+)[\'|"]\s'
def get_src_attrs(img_element):
    l = list()
    for i in img_element:
        l.append( re.findall(src_pat, i)[0] )
    return l
src_attrs = img_elements.map(lambda x: get_src_attrs(x))

#src_attrs.take(5)
#src_attrs.saveAsTextFile('src_attrs')
