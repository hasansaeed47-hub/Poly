import asyncio,json,hmac,hashlib,time
from collections import deque
from typing import Dict,List,Optional,Set
from datetime import datetime
import aiohttp,websockets

from config import ASSETS,get_creds
from core import Asset,TF,Market,Price,now,parse_asset,parse_tf,parse_target,parse_dir,calc_vol

class PriceFeed:
    WS_URL="wss://stream.binance.com:9443/stream"
    
    def __init__(self):
        self.bufs:Dict[Asset,Dict]={}
        for a in Asset:
            self.bufs[a]={"p1m":deque(maxlen=60),"p5m":deque(maxlen=300),"p1h":deque(maxlen=3600),"p4h":deque(maxlen=14400),"vol":deque(maxlen=3600),"last":0,"ts":None}
        self.running=False
        self._ws=None
    
    async def start(self):
        self.running=True
        while self.running:
            try:
                await self._connect()
            except Exception:
                if self.running:await asyncio.sleep(2)
    
    async def stop(self):
        self.running=False
        if self._ws:await self._ws.close()
    
    async def _connect(self):
        streams="/".join(f"{ASSETS[a.value]['sym'].lower()}@trade" for a in Asset)
        url=f"{self.WS_URL}?streams={streams}"
        async with websockets.connect(url) as ws:
            self._ws=ws
            async for msg in ws:
                if not self.running:break
                try:
                    d=json.loads(msg)
                    if "data" in d:self._proc(d)
                except:pass
    
    def _proc(self,d:dict):
        stream=d.get("stream","")
        data=d.get("data",{})
        for a in Asset:
            if ASSETS[a.value]["sym"].lower() in stream:
                p=float(data.get("p",0))
                v=float(data.get("q",0))
                if p>0:
                    b=self.bufs[a]
                    b["last"]=p
                    b["ts"]=now()
                    for k in ["p1m","p5m","p1h","p4h"]:b[k].append(p)
                    b["vol"].append(v)
                break
    
    def get(self,a:Asset)->Optional[Price]:
        b=self.bufs[a]
        if not b["last"] or not b["ts"]:return None
        def chg(dq):
            if len(dq)<2:return 0
            return (dq[-1]-dq[0])/dq[0] if dq[0]>0 else 0
        return Price(
            asset=a,price=b["last"],ts=b["ts"],
            c1m=chg(b["p1m"]),c5m=chg(b["p5m"]),c15m=chg(list(b["p5m"])[-900:]) if len(b["p5m"])>100 else 0,
            c1h=chg(b["p1h"]),c4h=chg(b["p4h"]),
            vol1h=sum(b["vol"]),vol2h=calc_vol(list(b["p1h"])[-7200:]) if len(b["p1h"])>100 else 0
        )
    
    def price(self,a:Asset)->float:
        return self.bufs[a]["last"]
    
    def vol(self,a:Asset)->float:
        return calc_vol(list(self.bufs[a]["p1h"])[-7200:]) if len(self.bufs[a]["p1h"])>100 else 0.01


class PolyAPI:
    BASE="https://clob.polymarket.com"
    
    def __init__(self):
        c=get_creds()
        self.key=c.pm_key
        self.secret=c.pm_secret
        self.passphrase=c.pm_pass
        self.paper=c.paper
        self.sess:Optional[aiohttp.ClientSession]=None
        self._cache:Dict[str,Market]={}
    
    async def init(self):
        self.sess=aiohttp.ClientSession()
    
    async def close(self):
        if self.sess:await self.sess.close()
    
    def _sign(self,ts:str,method:str,path:str,body:str="")->str:
        msg=f"{ts}{method}{path}{body}"
        return hmac.new(self.secret.encode(),msg.encode(),hashlib.sha256).hexdigest()
    
    def _headers(self,method:str,path:str,body:str="")->Dict:
        ts=str(int(time.time()*1000))
        return {"POLY_API_KEY":self.key,"POLY_SIGNATURE":self._sign(ts,method,path,body),"POLY_TIMESTAMP":ts,"POLY_PASSPHRASE":self.passphrase,"Content-Type":"application/json"}
    
    async def _req(self,method:str,ep:str,params:Dict=None,data:Dict=None,auth:bool=False)->Optional[Dict]:
        url=f"{self.BASE}{ep}"
        if params:url+=f"?{'&'.join(f'{k}={v}' for k,v in params.items())}"
        h={"Content-Type":"application/json"}
        body=""
        if data:body=json.dumps(data)
        if auth:h=self._headers(method,ep+(f"?{'&'.join(f'{k}={v}' for k,v in params.items())}" if params else ""),body)
        try:
            async with self.sess.request(method,url,headers=h,data=body if body else None) as r:
                if r.status==200:return await r.json()
        except:pass
        return None
    
    async def get_markets(self)->List[Dict]:
        r=await self._req("GET","/markets",{"active":"true"})
        return r.get("data",[]) if r else []
    
    async def get_crypto_markets(self)->List[Market]:
        all_m=await self.get_markets()
        markets=[]
        for m in all_m:
            q=m.get("question","")
            asset=parse_asset(q)
            tf=parse_tf(q)
            target=parse_target(q)
            if not all([asset,tf,target]):continue
            toks=m.get("tokens",[])
            yes_t=next((t for t in toks if t.get("outcome")=="Yes"),None)
            no_t=next((t for t in toks if t.get("outcome")=="No"),None)
            yes_p=float(yes_t.get("price",0.5)) if yes_t else 0.5
            no_p=float(no_t.get("price",0.5)) if no_t else 0.5
            try:res_t=datetime.fromisoformat(m.get("endDate","").replace("Z","+00:00"))
            except:res_t=now()
            mk=Market(
                id=m.get("id",""),cond_id=m.get("conditionId",""),question=q,
                asset=asset,tf=tf,target=target,direction=parse_dir(q),
                res_time=res_t,yes_p=yes_p,no_p=no_p,
                vol24=float(m.get("volume24hr",0)),liq=float(m.get("liquidity",0)),
                spread=abs(1-yes_p-no_p),created=now()
            )
            markets.append(mk)
            self._cache[mk.id]=mk
        return markets
    
    async def get_book(self,token_id:str)->Optional[Dict]:
        r=await self._req("GET","/book",{"token_id":token_id})
        if not r:return None
        bids=sorted([{"p":float(b.get("price",0)),"s":float(b.get("size",0))} for b in r.get("bids",[])],key=lambda x:-x["p"])
        asks=sorted([{"p":float(a.get("price",0)),"s":float(a.get("size",0))} for a in r.get("asks",[])],key=lambda x:x["p"])
        bb=bids[0]["p"] if bids else 0
        ba=asks[0]["p"] if asks else 1
        return {"bids":bids,"asks":asks,"bb":bb,"ba":ba,"spread":ba-bb,"mid":(bb+ba)/2,"bid_sz":sum(b["s"] for b in bids),"ask_sz":sum(a["s"] for a in asks)}
    
    async def place(self,token_id:str,side:str,price:float,size:float)->Optional[str]:
        if self.paper:return f"paper_{int(time.time()*1000)}"
        r=await self._req("POST","/order",data={"tokenID":token_id,"side":side,"price":str(price),"size":str(size),"type":"GTC"},auth=True)
        return r.get("orderID") if r else None
    
    async def market_order(self,token_id:str,side:str,size:float)->Optional[str]:
        book=await self.get_book(token_id)
        if not book:return None
        price=book["ba"] if side=="BUY" else book["bb"]
        return await self.place(token_id,side,price,size)
    
    async def cancel(self,order_id:str)->bool:
        if self.paper:return True
        r=await self._req("DELETE",f"/order/{order_id}",auth=True)
        return r is not None
    
    def get_cached(self,market_id:str)->Optional[Market]:
        return self._cache.get(market_id)
    
    async def refresh(self,market_id:str)->Optional[Market]:
        r=await self._req("GET",f"/markets/{market_id}")
        if not r:return None
        m=self._cache.get(market_id)
        if m:
            toks=r.get("tokens",[])
            for t in toks:
                if t.get("outcome")=="Yes":m.yes_p=float(t.get("price",m.yes_p))
                elif t.get("outcome")=="No":m.no_p=float(t.get("price",m.no_p))
            m.vol24=float(r.get("volume24hr",m.vol24))
            m.spread=abs(1-m.yes_p-m.no_p)
        return m


class Scanner:
    MIN_VOL={"15min":80000,"1hr":50000,"4hr":50000,"daily":50000}
    MIN_TIME={"15min":60,"1hr":300,"4hr":900,"daily":1800}
    
    def __init__(self,api:PolyAPI,interval:int=30):
        self.api=api
        self.interval=interval
        self.markets:Dict[Asset,Dict[TF,List[Market]]]={}
        self.running=False
    
    async def start(self):
        self.running=True
        while self.running:
            try:
                await self._scan()
            except:pass
            await asyncio.sleep(self.interval)
    
    async def stop(self):
        self.running=False
    
    async def _scan(self):
        from core import time_to_res
        mkts=await self.api.get_crypto_markets()
        self.markets={a:{tf:[] for tf in TF} for a in Asset}
        for m in mkts:
            wt=ASSETS.get(m.asset.value,{}).get("wt",{}).get(m.tf.value,0)
            if wt==0:continue
            if m.vol24<self.MIN_VOL.get(m.tf.value,50000):continue
            if time_to_res(m.res_time)<self.MIN_TIME.get(m.tf.value,60):continue
            self.markets[m.asset][m.tf].append(m)
    
    def all_markets(self)->List[Market]:
        return [m for a in self.markets.values() for tf in a.values() for m in tf]
    
    def get(self,a:Asset,tf:TF)->List[Market]:
        return self.markets.get(a,{}).get(tf,[])
