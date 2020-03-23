from pyspark import SparkContext, SparkConf
import requests
import string
import re
import os
import argparse


def get_html_file(url):
    f_name = url
    for char in string.punctuation:
        f_name = f_name.replace(char,'')
    f_name += '.html'
    p = os.path.join('downloads', f_name)
    page = requests.get(url)
    page.encoding = 'utf-8' #new
    html = page.text
    with open(p, 'w') as f: f.write(html)
    return f_name


def get_img_src(url):
    '''
    Due to error work Python ver diff than driver,
    doing os.environ... as well as config sc as local
    '''
    os.environ['PYSPARK_PYTHON']='python3.5'
    os.environ['PYSPARK_DRIVER_PYTHON']='python3.5'
    
    conf = SparkConf().setAppName('img src scrape').setMaster('local')
    sc = SparkContext(conf=conf)

    f_name = get_html_file(url)
    p = os.path.join('downloads', f_name)
    html_lines = sc.textFile(p)
    
    img_lines = html_lines.filter(lambda x: '<img ' in x)
    
    img_pat = r'<img[^>]+>'
    img_elements = img_lines.map(lambda x: re.findall(img_pat, x)[0])
    
    src_pat = r'src=[\'|"]([^\s]+)[\'|"]\s'
    src_attrs = img_elements.map(lambda x: re.findall(src_pat, x)[0])
    
    print('\nImage sources for {}:'.format(url))
    for i in src_attrs.collect(): print('  - ' + i)
    print('\n')
    
    p = os.path.join('collected', 'img_src_{}'.format(f_name.replace('.html','')))
    src_attrs.saveAsTextFile(p)


# CLI input
parser = argparse.ArgumentParser()
parser.add_argument(    '-url',
                        action='store',
                        dest='url',
                        help='Get URL\'s page\'s image sources.')

results = parser.parse_args()

if results.url != None:
    get_img_src(results.url)

