import sys, codecs, re, time, os
import urllib2

HOURLY_URL = 'http://finance.naver.com/item/sise_time.nhn?code=%s&thistime=%s150000&page=%d'
DAILY_URL = 'http://finance.naver.com/item/sise_day.nhn?code=%s&page=%d'

HOURLY_PATH = './hourly/%s/%s.csv'
HOURLY_DIR = './hourly/%s/'
DAILY_PATH = './daily/%s.csv'

INTERVAL = 1.5

def download(filename, url, waitOnError = True):
    while True:
        try:
            downloadFile = urllib2.urlopen(url)
        except:
            if waitOnError:
                print "Error occured! Wait 5 mins"
                time.sleep(60 * 5)
                continue
            else:
                break
        break

    print "Download file from: " + url
	
    output = open(filename,"wb")
    output.write(downloadFile.read())
    output.close()

def remove_tag(line):
    newLine = re.sub("<[^>]+>", " ", line).strip()
    newLine = re.sub(",", "", newLine).strip()

    return newLine

def extract_content(filename):
    PREFIX1 = '<td align="center"><span class="tah p10 gray03">'
    PREFIX2 = '<td class="num"><span class="tah p11">'

    result = {}
    sub_result = []
    current_key = None

    f = codecs.open(filename, "r", "cp949")

    for line in f:
        if line.strip().startswith(PREFIX1):
            current_key = remove_tag(line.strip())
        if line.strip().startswith(PREFIX2):
            sub_result += [remove_tag(line.strip())]

            if len(sub_result) >= 5:
                result[current_key] = sub_result
                sub_result = []

    f.close()

    return result

def crawl_daily(code, last_date = None):
    content = {}

    for page_index in range(999999):
        url = DAILY_URL % (code, page_index + 1)
        download('./tmp.html', url)
        extracted = extract_content('./tmp.html')

        if len(extracted) > 0:
            print extracted

            is_done = False
            for k, v in extracted.items():
                if (content.get(k) is None) and (last_date is None or k not in last_date):
                    content[k] = v
                else:
                    is_done = True

            if is_done:
                break
        else:
            break

        time.sleep(INTERVAL)

    return content

def crawl_hourly(code, date):
    content = []

    for page_index in range(37):
        url = HOURLY_URL % (code, date, page_index + 1)
        download('./tmp.html', url)
        extracted = dict_to_list(extract_content('./tmp.html'), reverse = True)

        if len(extracted) > 0:
            print extracted

            content += extracted
        else:
            break

        time.sleep(INTERVAL)

    content.reverse()
    return content

def get_last_date(filename, n = 15):
    f = codecs.open(filename, 'r', 'utf-8')

    dates = []
    for line in f:
        dates += [line.strip().split(',')[0]]

    f.close()

    return dates[-n:]

def append_data(filename, lines):
    f = open(filename, 'a')

    for line in lines:
        f.write(','.join(line) + '\n')

    f.close()

def dict_to_list(x, reverse = False):
    from operator import itemgetter

    y = []

    for k, v in x.items():
        y += [[k] + v]

    y = sorted(y, key = itemgetter(0), reverse = reverse)

    return y

def run_crawler(codes, endless = False):
    START_TIME = "16:00"

    while True:
        for code in codes:
            fn = DAILY_PATH % code

            last_date = get_last_date(fn) if os.path.exists(fn) else []
            append_data(fn, dict_to_list(crawl_daily(code, last_date = last_date)))
            time.sleep(INTERVAL)

            dates = get_last_date(fn, n = 99999)
            for date in dates:
                fn = HOURLY_PATH % (code, date)
                
                if not os.path.exists(HOURLY_DIR % code):
                    os.mkdir(HOURLY_DIR % code)

                if not os.path.exists(fn):
                    append_data(fn, crawl_hourly(code, date))
                    time.sleep(INTERVAL)

        if not endless:
            break

        while True:
            # check time

            time.sleep(50)

def get_codes(fn):
    codes = []

    f = codecs.open(fn, 'r', 'utf-8')

    for line in f:
        if line.strip() != "":
            codes += [line.strip().split(',')[1]]

    f.close()

    return codes[1:]

if __name__ == '__main__':
    codes = get_codes('./kospi.csv')
    run_crawler(codes)