import websocket
import time
import sys
import json
import hashlib
import zlib
import base64
import pandas as pd
from datetime import datetime
import time

MAX_DUMP = 100
ticker_col = ["timestamp", "sell", "buy", "last", "vol", "high", "low"]
ticker_file = "ticker_%s.csv"
ticker_idx = 0
depth_idx = 0
ticker_df = pd.DataFrame(columns=ticker_col)
start_time = datetime.now()


api_key='ae6af724-4cfa-433a-a89b-68ea73e1966b'
secret_key = "AB3BD4E9D1EB0D01C64EFB1140EDDC47"

#business
def buildMySign(params,secretKey):
    sign = ''
    for key in sorted(params.keys()):
        sign += key + '=' + str(params[key]) +'&'
    return  hashlib.md5((sign+'secret_key='+secretKey).encode("utf-8")).hexdigest().upper()

#spot trade
def spotTrade(channel,api_key,secretkey,symbol,tradeType,price='',amount=''):
    params={
      'api_key':api_key,
      'symbol':symbol,
      'type':tradeType
     }
    if price:
        params['price'] = price
    if amount:
        params['amount'] = amount
    sign = buildMySign(params,secretkey)
    finalStr =  "{'event':'addChannel','channel':'"+channel+"','parameters':{'api_key':'"+api_key+"',\
                'sign':'"+sign+"','symbol':'"+symbol+"','type':'"+tradeType+"'"
    if price:
        finalStr += ",'price':'"+price+"'"
    if amount:
        finalStr += ",'amount':'"+amount+"'"
    finalStr+="},'binary':'true'}"
    return finalStr

#spot cancel order
def spotCancelOrder(channel,api_key,secretkey,symbol,orderId):
    params = {
      'api_key':api_key,
      'symbol':symbol,
      'order_id':orderId
    }
    sign = buildMySign(params,secretkey)
    return "{'event':'addChannel','channel':'"+channel+"','parameters':{'api_key':'"+api_key+"','sign':'"+sign+"','symbol':'"+symbol+"','order_id':'"+orderId+"'},'binary':'true'}"

#subscribe trades for self
def realtrades(channel,api_key,secretkey):
   params={'api_key':api_key}
   sign=buildMySign(params,secretkey)
   return "{'event':'addChannel','channel':'"+channel+"','parameters':{'api_key':'"+api_key+"','sign':'"+sign+"'},'binary':'true'}"

# trade for future
def futureTrade(api_key,secretkey,symbol,contractType,price='',amount='',tradeType='',matchPrice='',leverRate=''):
    params = {
      'api_key':api_key,
      'symbol':symbol,
      'contract_type':contractType,
      'amount':amount,
      'type':tradeType,
      'match_price':matchPrice,
      'lever_rate':leverRate
    }
    if price:
        params['price'] = price
    sign = buildMySign(params,secretkey)
    finalStr = "{'event':'addChannel','channel':'ok_futuresusd_trade','parameters':{'api_key':'"+api_key+"',\
               'sign':'"+sign+"','symbol':'"+symbol+"','contract_type':'"+contractType+"'"
    if price:
        finalStr += ",'price':'"+price+"'"
    finalStr += ",'amount':'"+amount+"','type':'"+tradeType+"','match_price':'"+matchPrice+"','lever_rate':'"+leverRate+"'},'binary':'true'}"
    return finalStr

#future trade cancel
def futureCancelOrder(api_key,secretkey,symbol,orderId,contractType):
    params = {
      'api_key':api_key,
      'symbol':symbol,
      'order_id':orderId,
      'contract_type':contractType
    }
    sign = buildMySign(params,secretkey)
    return "{'event':'addChannel','channel':'ok_futuresusd_cancel_order','parameters':{'api_key':'"+api_key+"',\
            'sign':'"+sign+"','symbol':'"+symbol+"','contract_type':'"+contractType+"','order_id':'"+orderId+"'},'binary':'true'}"

#subscribe future trades for self
def futureRealTrades(api_key,secretkey):
    params = {'api_key':api_key}
    sign = buildMySign(params,secretkey)
    return "{'event':'addChannel','channel':'ok_usd_future_realtrades','parameters':{'api_key':'"+api_key+"','sign':'"+sign+"'},'binary':'true'}"

def on_open(self):
    #subscribe okcoin.com spot ticker
    #self.send("{'event':'addChannel','channel':'ok_btcusd_ticker','binary':'true'}")
    self.send("{'event':'addChannel','channel':'ok_btccny_ticker','binary':'true'}")
    #self.send("{'event':'addChannel','channel':'ok_btccny_depth','binary':'true'}")

    #subscribe okcoin.com future this_week ticker
    #self.send("{'event':'addChannel','channel':'ok_btcusd_future_ticker_this_week','binary':'true'}")

    #subscribe okcoin.com future depth
    #self.send("{'event':'addChannel','channel':'ok_ltcusd_future_depth_next_week','binary':'true'}")

    #subscrib real trades for self
    #realtradesMsg = realtrades('ok_usd_realtrades',api_key,secret_key)
    #self.send(realtradesMsg)


    #spot trade via websocket
    #spotTradeMsg = spotTrade('ok_spotusd_trade',api_key,secret_key,'ltc_usd','buy_market','1','')
    #self.send(spotTradeMsg)


    #spot trade cancel
    #spotCancelOrderMsg = spotCancelOrder('ok_spotusd_cancel_order',api_key,secret_key,'btc_usd','125433027')
    #self.send(spotCancelOrderMsg)

    #future trade
    #futureTradeMsg = futureTrade(api_key,secret_key,'btc_usd','this_week','','2','1','1','20')
    #self.send(futureTradeMsg)

    #future trade cancel
    #futureCancelOrderMsg = futureCancelOrder(api_key,secret_key,'btc_usd','65464','this_week')
    #self.send(futureCancelOrderMsg)

    #subscrbe future trades for self
    #futureRealTradesMsg = futureRealTrades(api_key,secret_key)
    #self.send(futureRealTradesMsg)

def dump_ticker(data):
    if data is None:
        return

    global ticker_df, ticker_idx, start_time
    timestamp = int(data.get("timestamp"))
    sell      = float(data.get("sell"))
    buy       = float(data.get("buy"))
    last      = float(data.get("last"))
    vol       = float(data.get("vol").replace(",", ""))
    high      = float(data.get("high"))
    low       = float(data.get("low"))

    data_dict = {"timestamp":timestamp,
                 "sell":sell,
                 "buy":buy,
                 "last":last,
                 "vol":vol,
                 "high":high,
                 "low":low}

    try:
        ticker_df = ticker_df.append(data_dict, ignore_index=True)
        ticker_idx += 1
        # print("dump_ticker:{0} {1}".format(datetime.fromtimestamp(timestamp/1000.0).ctime(), ticker_idx % MAX_DUMP))
        if ticker_idx % MAX_DUMP == 0:
            file_name = ticker_file % datetime.now().strftime("%Y%m%d")
            ticker_df.to_csv(file_name,
                             mode="a", 
                             index=False, 
                             header=True if ticker_idx == MAX_DUMP else False)
            ticker_df = ticker_df[:0] # delete all rows

            """statis"""
            now = datetime.now()
            dur = (now - start_time).total_seconds()
            print("dump ticker:dur:{0}, count:{1}, mean:{2}".format(dur, ticker_idx, MAX_DUMP/dur))
            start_time = now
            # print ticker_df.head()
    except Exception,e:
        print "dump_ticker", e


def on_message(self,evt):
    records = inflate(evt)
    if records is None:
        return

    if len(records) > 1:
        print len(records)
    for rd in records:
        channel = rd.get("channel")
        data    = rd.get("data")
        if channel == "ok_btccny_ticker":
            dump_ticker(data)
        elif channel == "ok_btccny_depth":
            dump_depth(data)


def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()

    try:
        info = json.loads(inflated)
        return info
    except Exception,e:
        print(e)
        return None

def on_error(self,evt):
    print("on_error", evt)

def on_close(self,evt):
    print ('DISCONNECT')

if __name__ == "__main__":
    #url = "wss://real.okcoin.com:10440/websocket/okcoinapi"      #if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    url = "wss://real.okcoin.cn:10440/websocket/okcoinapi"      #if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    #api_key='your api_key which you apply'
    #secret_key = "your secret_key which you apply"

    websocket.enableTrace(False)
    if len(sys.argv) < 2:
        host = url
    else:
        host = sys.argv[1]
    ws = websocket.WebSocketApp(host,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
