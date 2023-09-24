
import sys,os
import sql
import pandas as pd
from datetime import datetime, timedelta


datas = pd.DataFrame()

# 52주 신고가 여부 판단 함수
def is_52_week_high(row):
    if( row['52max'] == 0):
        return 0
    if row['Close'] >= row['52max']:
        return 1
    else:
        return 0

def get_data(symbols):

    dd = {}
    for symbol in symbols:
        df = sql.DBM().data(symbol) 
        df['52max'] = df['Close'].rolling(window=250).max()
        df['52max'].fillna(0, inplace=True)
        df['52max_or_not'] = df.apply(is_52_week_high, axis=1)
        df['52max_or_not_before'] = df['52max_or_not'].shift(1)
        df['High_after'] = df['High'].shift(-1)
        df['Low_after'] = df['Low'].shift(-1)
        df['Close_after'] = df['Close'].shift(-1)
        df['Open_after'] = df['Open'].shift(-1)
        df['High_after'].fillna(0,inplace=True)
        df['Low_after'].fillna(0,inplace=True)
        df['Close_after'].fillna(0,inplace=True)
        df['Open_after'].fillna(0,inplace=True)
        dd[symbol] = df
        # print(df[['Close', '52max', '52max_or_not']])
        # df.to_csv(symbol + '.csv')
    # 딕셔너리의 값들을 리스트로 모읍니다.
    dataframes = list(dd.values())
    # 리스트에 있는 데이터프레임들을 위아래로 결합합니다.
    datas = pd.concat(dataframes, axis=0)
    return datas

def run( symbols, df ):

    date_list = df.index.tolist()
    date_list = sorted( list(set(date_list)) )

    won = 0
    lose = 0
    cash = 1000000

    max_slot = 3

    for i in range(1, len(date_list)):

        previous_date = date_list[i - 1]
        current_date = date_list[i]

        previous_data = df.loc[previous_date]
        current_data = df.loc[current_date]

        filtered_data = df[(df.index == current_date) & (df['52max_or_not'] == 1) & (df['52max_or_not_before'] == 0)]
        if( filtered_data.empty ):
            continue
        # 결과 출력
            
        earning_rate = 1.02
        lose_rate = 0.95

        try:
            
            max_position = min( max_slot, len( filtered_data ) )
            max_price = cash/max_position
            
            # max_price = cash
            cc = 0
            filtered_data = filtered_data.sort_values(by=['acml_tr_pbmn'], ascending=False)
            for index, row in filtered_data.iterrows():
                cc = cc + 1
                # print(f"Index: {index}, Value: {row}")

                if( cc > max_position ):
                    break

                symbol, buy_price, high_after, low_after, close_after, open_after = str( row['symbol'] ), int(row['Close']), int(row['High_after']), int(row['Low_after']) , int(row['Close_after']), int(row['Open_after'])
                sell_price = 0
                if( high_after == 0 ):
                    sell_price = buy_price
                else:
                    if( open_after > int(buy_price*earning_rate) ):
                        sell_price = int(buy_price*earning_rate)
                    elif( high_after > int(buy_price*earning_rate) ):
                        sell_price = int(buy_price*earning_rate)
                    elif( low_after < int(buy_price*lose_rate) ):
                        sell_price = int(buy_price*lose_rate)
                    else: 
                        sell_price = close_after


                # print( symbol, close, after, (1-(after/close))*100) )
                # print(filtered_data[['symbol', 'Close', 'High_after']])
                
                slot = (int(max_price/buy_price))
                percentage_change = ((sell_price / buy_price)-1) * 100
                # sell_price = int(close * percentage_change / 100) + close
                gain = ( sell_price - buy_price ) * slot
                cash = cash + gain

                # percentage_change = ((after / close)-1) * 100
                output_str = f"{current_date}: Symbol: {symbol}, Buy: {buy_price}, Sell: {sell_price}, {percentage_change:.2f}%, gain:{gain:,}, Cash: {cash:,}"
                print(output_str)

                if( percentage_change > 0.0) :
                    won = won + 1
                else :
                    lose = lose + 1
    
        except TypeError as e:
            print(f"An exception occurred: {e}")
            print(row)
            # print(filtered_data[['symbol', 'Close', 'High_after']])


    print( f"won : {won}, lose : {lose}")
    # start_str = '2011-01-01'
    # end_str = '2015-01-01'
    # date_format = "%Y-%m-%d"
    
    # start_date = datetime.strptime(start_str, date_format)
    # end_date = datetime.strptime(end_str, date_format)

    # dates = []
    # # 변환된 날짜 객체 출력
    # current_date = start_date  # 시작 날짜로 초기화
    # while current_date <= end_date:
    #     dates.append(current_date)
    #     # 현재 날짜를 출력하거나 필요한 작업 수행
    #     # print(current_date.strftime(date_format))  # 날짜를 문자열로 출력
    #     # 다음 날짜로 이동
    #     current_date += timedelta(days=1)

    #     formatted_date = current_date.date().strftime("%Y-%m-%d")
    #     print(formatted_date)
    #     print(df.columns)
    #     print(df.loc[ '2011-01-02' ])
    #     print(df.loc[ current_date.date() ] )
    #     break


if __name__ == '__main__':
    symbols = sql.DBM().get_symbols_by_top(200)
    # symbols = symbols.iloc[99:]

    df = get_data(symbols['symbol'] )
    run( symbols['symbol'], df)
