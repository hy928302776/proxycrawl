import datetime

if __name__ == '__main__':
    datetime_str ='2023-09-07 14:01:04'

    print (int(datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S').timestamp()))

    print(int(datetime.datetime.combine(datetime.datetime.today(),datetime.time.min).timestamp()))
    print(int(datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=1)),datetime.time.min).timestamp()))

    print(datetime.datetime.fromtimestamp(1694448000).strftime('%Y-%m-%d %H:%M:%S'))