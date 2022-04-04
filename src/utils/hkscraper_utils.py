def getTickerCode(ticker_list):
    temp = list()
    for ticker in ticker_list:
        if len(ticker) == 6:
            suffix = ".SZ"
        elif len(ticker) == 5:
            # Remove the frist index
            ticker = ticker[1:5]
            suffix = ".HK"
        
        temp.append(ticker+suffix)
    return temp