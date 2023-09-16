import datetime

if __name__ == '__main__':
    beginDateStr =None
    endDate = (datetime.date.today() - datetime.timedelta(days=1)) if not beginDateStr else datetime.datetime.strptime(
        beginDateStr, "%Y-%m-%d").date()
    print(endDate)
