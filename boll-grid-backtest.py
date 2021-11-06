# coding=utf-8
import sys
import asyncio
import aiohttp
import json
from datetime import datetime
import pandas as pd
import time
import csv
import codecs
import math
import csv
import random
import string



base_url = "https://api.binance.com/api/v3/"
kline_req_url = base_url+"klines"
itv='15m'



instruments=['BTC',
    'ETH',
    'BNB',
    'SOL',
    'FTT',
    'DOT',
    'ADA',
    'DOGE',
]

async def request(session,url):
    headers = {'content-type': 'application/json'}
    async with session.get(url,headers=headers) as res:
        return await res.text()

async def collectdata_calc():    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        now = datetime.now()
        ONE_DAY = 86400000
        endTime = int(time.mktime(now.timetuple())*1e3)
        for ins in instruments:
            startTime = 1502942400000 # 2017年8月17日星期四 12:00:00
            # collect kline 
            t = startTime
            all_klines = []
            while(t <= endTime):
                kline_url = kline_req_url+'?symbol='+ins+'USDT&interval='+itv+'&startTime='+str(t)+'&limit=1000'
                retr = await request(session,kline_url)
                arrobj = json.loads(retr)
                if(len(arrobj)<1):return
                startTime = arrobj[0][0]
                if(len(arrobj)<1):
                    if(fundend<1):fundend = t
                    break
                t = arrobj[-1][6]+10 #
                all_klines += arrobj
            
            df = pd.DataFrame(all_klines)
            df.to_csv(ins+'_15m.csv')


def variance(data, ddof=0):
    n = len(data)
    mean = sum(data) / n
    return sum((x - mean) ** 2 for x in data) / (n - ddof)
def stdev(data):
    var = variance(data)
    std_dev = math.sqrt(var)
    return std_dev


boll_cnt = 20 
boll_std = 2
riskratio = 0.03
leverage = 2
initfund = 100000
C24 = 1.01
C25 = 1
takeprofit_ratio = 0.01

def calc_entry_price_boll(klines,cnt,boll_cnt,boll_std):
    closearr = []
    for k in range(len(klines)):
        if(k%cnt == 0):
            closearr.append(float(klines[k][5]))
        if(len(closearr)>=boll_cnt):
            break
    mean = sum(closearr) / len(closearr)
    std = stdev(closearr)
    upper = mean + std * boll_std
    lower = mean - std * boll_std
    return upper,lower


def wen_strategy(klines,notiontotal,compoundfund):
    bEnter = False
    entryprice = float(klines[0][2])
    sz = 0
    curprice = float(klines[-1][5])
    C5 = curprice
    cnt_day = int(24*60/15)
    cnt_6hr = int(6*60/15)
    cnt_4hr = int(4*60/15)
    cnt_1hr = int(60/15)
    cnt_15m = 1
    upper_day,lower_day = calc_entry_price_boll(klines,cnt_day,boll_cnt,boll_std)
    upper_4hr,lower_4hr = calc_entry_price_boll(klines,cnt_4hr,boll_cnt,boll_std)
    upper_6hr,lower_6hr = calc_entry_price_boll(klines,cnt_6hr,boll_cnt,boll_std)
    upper_1hr,lower_1hr = calc_entry_price_boll(klines,cnt_1hr,boll_cnt,boll_std)
    upper_15m,lower_15m = calc_entry_price_boll(klines,cnt_15m,boll_cnt,boll_std)
    
    E5 = upper_day
    E8 = upper_6hr
    C30 = E5+E8
    C32 = upper_4hr
    C34 = upper_1hr
    C36 = upper_15m
    D31 = (C30+C32)/2
    D33 = (C32+C34)/2
    D35 = (C34+C36)/2
    E32 = (D31+D33)/2
    E34 = (D33+D35)/2
    F33 = (E32+E34)/2
    
    
    H5 = lower_day
    H8 = lower_6hr
    H17 = lower_15m
    
    C39 = H5+H8
    C41 = lower_4hr
    C43 = lower_1hr
    C45 = lower_15m
    D40 = (C39+C41)/2
    D42 = (C41+C43)/2
    D44 = (C43+C45)/2
    E41 = (D40+D42)/2
    E43 = (D42+D44)/2
    F42 = (E41+E43)/2
    
    E23 = (F33+C5)/2*C24
    E27 = (F42+C5)/2*C24
    E25 = (E23+E27)/2
    E24 = (E23+E25)/2
    E26 = (E25+E27)/2
    
    entryprice = curprice
    if(curprice>E24):
        riskratio = (E23/E24)-1
        entryprice = E24
    elif(curprice>E25):
        riskratio = (E23/E25)-1
        entryprice = E25
    elif(curprice>E26):
        riskratio = (E23/E26)-1
        entryprice = E26
    else:
        riskratio = (E23/E27)-1
        entryprice = curprice
    
    profit_ratio = riskratio * C24 
    if((notiontotal/compoundfund)<riskratio):
        sz = compoundfund * riskratio * leverage / entryprice
        bEnter = True
    
    return bEnter,entryprice,sz,profit_ratio
    


def backTestFromCsv(ins):
    ONE_DAY = 86400000
    FIVE_DAYS = 86400000*5

    all_klines = []
    with open(ins+'_15m.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    
    compoundfund = initfund
    margin = compoundfund
    positiveFundTimes = 0
    totalTradeTimes = 0
    hh = -9999
    dd = 9999
    mdd = 9999
    prvdaynetprofit = -9999
    dmdd = 9999
    lastHHTimestamp = 0
    longestHHPeriod = -9999
    avgdailyrate = 0
    prvcompfund = 0
    avgvolatility = 0
    
    
    timestart = float(all_klines[0][1])
    timeend = float(all_klines[-1][1])
    daystamp = timestart
    dailyrate = []
    curposition = []
    history_orders = []
    trade_records = []
    for k in range(len(all_klines)-1):
        kl = all_klines[k]
        curtime = float(kl[1])
        if(curtime - timestart < FIVE_DAYS):continue
        if(curtime - daystamp >= ONE_DAY):

            daystamp = curtime
            curprice = float(kl[5])
            for p in range(len(curposition)-1,-1,-1):
                po = curposition[p]
                if(po['filled']<0.001):
                    history_orders.append(curposition.pop(len(curposition)-1))
            
            notiontotal = 0
            for po in curposition:
                notiontotal += curprice*po['size']
            
            daynetprofit = 0
            positionNotion = 0
            if(len(curposition)>0):
                for po in curposition:
                    if( (abs(po['filled'])>0.0001) ):
                        daynetprofit += (curprice - po['entryprice']) * po['filled']
                        positionNotion += curprice * po['filled']
                compoundfund = margin + positionNotion
            
            
            if(prvcompfund<1):prvcompfund = compoundfund
            todaynetprofit = compoundfund-prvcompfund
            d_dd = todaynetprofit-prvdaynetprofit
            if(d_dd < dmdd):dmdd = d_dd # d_dd
            prvdaynetprofit = todaynetprofit            
            
            
            if(compoundfund > hh):
                hh=compoundfund
                if(lastHHTimestamp<1):lastHHTimestamp=curtime
                period = (curtime - lastHHTimestamp)
                if(period > longestHHPeriod):
                    longestHHPeriod = period
                lastHHTimestamp = curtime
            elif(compoundfund < hh):
                dd = compoundfund - hh
                if(dd < mdd):mdd=dd
            
            avgvolatility += abs(todaynetprofit) / prvcompfund
            profitrate = d_dd/prvcompfund
            dailyrate.append(profitrate)
            avgdailyrate += profitrate
            prvcompfund = compoundfund
            
            numbar = min(1920,k)
            bEnter,entryprice,sz,profit_ratio = wen_strategy(all_klines[k-numbar:k+1],notiontotal,compoundfund)
            if(bEnter):
                orderid = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
                po = {'entryprice':entryprice,'exitprice':-1,'size':sz,'filled':0,'orderid':orderid,'time':curtime}
                takeprofit_ratio = profit_ratio
                curposition.append(po)
                
            trade_record = {}
            trade_record['time'] = curtime
            trade_record['equity'] = compoundfund 
            trade_record['price'] = curprice
            trade_records.append(trade_record)

        for po in curposition:
            curlow = float(kl[4])
            if( (abs(po['filled'])<0.0001) ):
                if(curlow <= po['entryprice']):
                    po['filled'] = po['size']
                    margin = compoundfund - entryprice * sz

        avgEntryPrice = 0
        entrySizeTotal = 0
        runtime_profit = 0
        curhigh = float(kl[3])
        for po in curposition:
            if(po['filled']>0.001):
                entrySizeTotal += po['filled']
                avgEntryPrice += po['entryprice'] * po['filled']
        
        if(avgEntryPrice>0):
            avgEntryPrice /= entrySizeTotal
            if( (curhigh-avgEntryPrice)/avgEntryPrice > takeprofit_ratio ):
                runtime_profit = (avgEntryPrice * takeprofit_ratio) * po['filled']
                positionNotion = 0
                for po in curposition:
                    exitprice = avgEntryPrice*(1+takeprofit_ratio)
                    po['exitprice'] = exitprice 
                    positionNotion += exitprice * po['filled']
                    history_orders.append(po)
                compoundfund = margin + positionNotion
                curposition = []
                totalTradeTimes += 1
                positiveFundTimes += 1
    

    avgdailyrate /= len(dailyrate)
    avgvolatility /= len(dailyrate)
    winrate = (positiveFundTimes / totalTradeTimes)*100
    

    sharpe = avgdailyrate / stdev(dailyrate)
    
    retdict={}
    retdict['timestart'] = timestart
    retdict['timeend'] = timeend
    retdict['positiveFundTimes'] = positiveFundTimes
    retdict['totalTradeTimes'] = totalTradeTimes
    retdict['compoundfund'] = compoundfund
    retdict['winrate'] = winrate
    retdict['longestHHPeriod'] = longestHHPeriod/86400000
    retdict['mdd'] = (mdd/initfund)*100
    retdict['dmdd'] = (dmdd/initfund)*100
    retdict['sharpe'] = sharpe
    retdict['avgvolatility'] = avgvolatility
    
    
    return retdict,trade_records
    
def backtest():
    for ins in instruments:
        fundhist,trade_records = backTestFromCsv(ins)
        totalReportFilename = ins + '_rpt.txt'
        
        file_object = codecs.open( totalReportFilename , 'w', "utf-8")
        file_object.write('')
        file_object.close()
        
        starttime = fundhist['timestart'] / 1000
        endtime = fundhist['timeend'] / 1000
        starttime_dt = datetime.fromtimestamp(starttime)
        endtime_dt = datetime.fromtimestamp(endtime)
        
        durationDays = (fundhist['timeend'] - fundhist['timestart']) / 86400000
        onedayret = (fundhist['compoundfund'] - initfund)/durationDays/initfund
        yearret = onedayret * 365 * 100
        yearret_str = "{:.2f}".format(yearret)
        
        positiveRatio = (fundhist['positiveFundTimes'] / fundhist['totalTradeTimes'])*100
        positiveRatio_str = "{:.2f}".format(positiveRatio)
        
        netReturn = fundhist['compoundfund'] - initfund
        netReturn_str = "{:.2f}".format(netReturn)
        grossRate = (netReturn / initfund)/durationDays*365*100
        grossRate_str = "{:.2f}".format(grossRate)
        
        msg = u''
        msg += ins + 'USDT \n'
        msg += u'回測時間:'+str(starttime_dt)+' to '+str(endtime_dt)+ ' 共 ' +str(int(durationDays)) + ' 天 \n'
        msg += u'初始資金:'+ str(initfund) +'USD\n'
        msg += u'賺錢次數:'+ str(fundhist['positiveFundTimes'])+'\n'
        msg += u'總交易次數:'+ str(fundhist['totalTradeTimes'])+'\n'
        msg += u'勝率:'+ positiveRatio_str +'%\n'
        msg += u'總利潤:'+ netReturn_str +'USD\n'
        msg += u'最大創高區間:'+ ("{:.2f}".format(fundhist['longestHHPeriod'])) +'天\n'
        msg += u'最大拉回:'+ ("{:.2f}".format(fundhist['mdd'])) +'%\n'
        msg += u'每日最大拉回:'+ ("{:.2f}".format(fundhist['dmdd'])) +'%\n'
        msg += u'夏普比率:'+ ("{:.2f}".format(fundhist['sharpe'])) +'\n'
        msg += u'波動率:'+ ("{:.2f}".format(fundhist['avgvolatility'])) +'%\n'
        msg += u'每月報酬:'+ ("{:.2f}".format(grossRate/12.0)) +'%\n'
        msg += u'年化報酬:'+ grossRate_str +'%\n\n'
        
        
        file_object = codecs.open(totalReportFilename, 'a', "utf-8")
        file_object.write(msg)
        file_object.close()        
        

        with open( (ins+'_records.csv'), mode='w') as fprice_file:
            fprice_file = csv.writer(fprice_file , lineterminator='\n',  delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            fprice_file.writerow(['time','equity','price'])
        
            prvrate = 0
            prvnetprofit = 0
            prvprice = 0
            for rec in trade_records:
                _dt = datetime.fromtimestamp( rec['time'] /1000)
                fprice_file.writerow([_dt,rec['equity'],rec['price']])
        

if __name__ == "__main__":
    mode = sys.argv[1]
    if(mode=='0'):
        asyncio.run(collectdata_calc())
    else:
        backtest()
