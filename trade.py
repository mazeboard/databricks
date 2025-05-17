import os
import random
from datetime import datetime
import pandas as pd

data = {}
for file in list(filter(lambda x:x.endswith('.csv'), os.listdir('nasdaq'))):
        iter = pd.read_csv(f"nasdaq/{file}").iterrows()
        while True:
            try:
                i,row=next(iter)
                d = data.get(row['date'])
                ticker = file.replace('.csv','')
                entry = {'open': row['open'], 'high': row['high'], 'low': row['low'], 'close': row['close'], 'volume': row['volume']}
                if d==None:
                    x = {}
                    x[ticker]=entry
                    data[row['date']] = x
                    
                else:
                    data[row['date']][ticker] = entry
            except StopIteration:
                break
len(list(data.keys()))

maxQTY=40000
minVol=1000000
amt=5000
ngroups=10

def split_dict(d, n):
    items = list(d.items())
    random.shuffle(items)
    k, m = divmod(len(items), n)
    return list(map(lambda items: dict(items), 
               [items[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]))

def buy(amt, quote):
    o=quote['open']
    qty=min(maxQTY, 100*int(amt/o/100))
    return {'open':o, 'qty':qty, 'balance':amt-qty*o,'last':o}

def filter_dict_by_volume(d):
    return dict(filter(lambda item:item[1]['volume']>minVol, d.items()))
    
def filter_data(data, tickers):
    return dict([(k, dict(filter(lambda item: item[0] in tickers, list(quotes.items()))))
                 for (k, quotes) in data.items()])
    
def simul(data, positions):
    ticker = list(positions.keys())[0]
    pos = positions.get(ticker)
    value = pos['last']*pos['qty']+pos['balance']
    print("---- simul", ticker, len(positions.keys()))
    pvalue=value
    dates = list(data.keys())[1:]
    for d in dates:
        pos = positions.get(ticker)
        value = pos['last']*pos['qty']+pos['balance']
        pos['last']=data.get(d).get(ticker)['open']
        value=pos['qty']*pos['last']+pos['balance']
        delta=0
        s=()
        for name, quote in data.get(d).items():
            if (name!=ticker): # remove this condition to be able to buy more shares of the current selection
                q = buy(value, quote)
                p = positions.get(name)
                #print(f"{d}: {ticker} {value} - {name} q:{q} p:{p}")
                if quote['volume'] > minVol & q['qty'] > p['qty'] and q['qty'] - p['qty'] > delta:
                    delta=q['qty'] - p['qty']
                    s=(name, q)
        if (s!=()):
            print(f"{d}: {ticker} => {name}x{q['qty']} - {value}")
            p = s[1]
            ticker=s[0]
            positions[ticker]=p
            pvalue=p['qty']*p['open']+p['balance']
    pos=positions.get(ticker)
    value=pos['qty']*pos['last']+pos['balance']
    return (ticker, value, pos)

period_size=3 # month
months=sorted(set(map(lambda x:x[:7], data.keys())))
periods = list(zip(months[:-period_size],months[period_size:]))

gains = []
for start, end in periods:
    print(f"******* period {start} - {end}")
    selected_data = dict(filter(lambda x:x[0]>=start and x[0]<end, data.items()))
    
    dates=list(selected_data.keys())
    dates.sort()
    
    splitted_dict = split_dict(selected_data.get(dates[0]), ngroups)
    ps=[dict([(ticker, buy(amt, entry)) for (ticker, entry) in filter_dict_by_volume(d).items()])
        for d in splitted_dict]
    positions=[dict(filter(lambda x:x[1]['qty']>0, p.items())) for p in ps]
    print(positions)
    r=[simul(filter_data(selected_data, list(p.keys())), p) for p in positions]
    for i in r: print(i)
    s=sum(map(lambda x: x[1], r))
    gains.append((start, end, s-ngroups*amt, int(10000*(s-ngroups*amt)/(ngroups*amt))/100))
total_gains=0
for g in gains:
    total_gains += g[2]
    print(g)
print("total_gains",total_gains)
