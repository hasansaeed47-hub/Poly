import asyncio,json,hmac,hashlib,time,logging,re
from collections import deque
from typing import Dict,List,Optional,Set
from datetime import datetime,timezone
import aiohttp,websockets

from config import ASSETS,GAMMA_API,CLOB_API,SCAN_INTERVAL,GAMMA_FETCH_LIMIT,get_creds
from core import (Asset,TF,Market,Price,now,parse_asset,parse_tf,parse_target,
                  parse_dir,calc_vol,time_to_res,ASSET_KEYWORDS)

log=logging.getLogger(__name__)


class PriceFeed:
    WS_URL="wss://stream.binance.com:9443/stream"

    def __init__(self):
        self.bufs:Dict[Asset,Dict]={}
        for a in Asset:
            self.bufs[a]={"p1m":deque(maxlen=60),"p5m":deque(maxlen=300),
                          "p1h":deque(maxlen=3600),"p4h":deque(maxlen=14400),
                          "vol":deque(maxlen=3600),"last":0,"ts":None}
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
            c1m=chg(b["p1m"]),c5m=chg(b["p5m"]),
            c15m=chg(list(b["p5m"])[-900:]) if len(b["p5m"])>100 else 0,
            c1h=chg(b["p1h"]),c4h=chg(b["p4h"]),
            vol1h=sum(b["vol"]),vol2h=calc_vol(list(b["p1h"])[-7200:]) if len(b["p1h"])>100 else 0
        )

    def price(self,a:Asset)->float:
        return self.bufs[a]["last"]

    def vol(self,a:Asset)->float:
        return calc_vol(list(self.bufs[a]["p1h"])[-7200:]) if len(self.bufs[a]["p1h"])>100 else 0.01


class PolyAPI:
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
        return {"POLY_API_KEY":self.key,"POLY_SIGNATURE":self._sign(ts,method,path,body),
                "POLY_TIMESTAMP":ts,"POLY_PASSPHRASE":self.passphrase,"Content-Type":"application/json"}

    async def _clob(self,method:str,ep:str,params:Dict=None,data:Dict=None,auth:bool=False)->Optional[Dict]:
        url=f"{CLOB_API}{ep}"
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

    async def _gamma(self,ep:str,params:Dict=None)->Optional[any]:
        url=f"{GAMMA_API}{ep}"
        if params:url+=f"?{'&'.join(f'{k}={v}' for k,v in params.items())}"
        try:
            async with self.sess.get(url) as r:
                if r.status==200:return await r.json()
        except:pass
        return None

    async def get_book(self,token_id:str)->Optional[Dict]:
        r=await self._clob("GET","/book",{"token_id":token_id})
        if not r:return None
        bids=sorted([{"p":float(b.get("price",0)),"s":float(b.get("size",0))} for b in r.get("bids",[])],key=lambda x:-x["p"])
        asks=sorted([{"p":float(a.get("price",0)),"s":float(a.get("size",0))} for a in r.get("asks",[])],key=lambda x:x["p"])
        bb=bids[0]["p"] if bids else 0
        ba=asks[0]["p"] if asks else 1
        return {"bids":bids,"asks":asks,"bb":bb,"ba":ba,"spread":ba-bb,"mid":(bb+ba)/2,
                "bid_sz":sum(b["s"] for b in bids),"ask_sz":sum(a["s"] for a in asks)}

    async def place(self,token_id:str,side:str,price:float,size:float)->Optional[str]:
        if self.paper:return f"paper_{int(time.time()*1000)}"
        r=await self._clob("POST","/order",data={"tokenID":token_id,"side":side,
                           "price":str(price),"size":str(size),"type":"GTC"},auth=True)
        return r.get("orderID") if r else None

    async def market_order(self,token_id:str,side:str,size:float)->Optional[str]:
        book=await self.get_book(token_id)
        if not book:return None
        price=book["ba"] if side=="BUY" else book["bb"]
        return await self.place(token_id,side,price,size)

    async def cancel(self,order_id:str)->bool:
        if self.paper:return True
        r=await self._clob("DELETE",f"/order/{order_id}",auth=True)
        return r is not None

    def get_cached(self,market_id:str)->Optional[Market]:
        return self._cache.get(market_id)

    async def refresh(self,market_id:str)->Optional[Market]:
        r=await self._clob("GET",f"/markets/{market_id}")
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

    # --- Gamma API event/market discovery ---

    async def fetch_events_by_tag(self,tag_slug:str,limit:int=100)->List[Dict]:
        r=await self._gamma("/events",{"tag_slug":tag_slug,"active":"true",
                                        "closed":"false","limit":str(limit)})
        return r if isinstance(r,list) else []

    async def fetch_event_by_slug(self,slug:str)->Optional[Dict]:
        r=await self._gamma(f"/events/slug/{slug}")
        return r if isinstance(r,dict) else None

    async def fetch_markets_by_slug(self,slug:str)->Optional[Dict]:
        r=await self._gamma(f"/markets/slug/{slug}")
        return r if isinstance(r,dict) else None

    def cache_market(self,m:Market):
        self._cache[m.id]=m


def _classify_event_tf(slug:str,title:str)->Optional[TF]:
    sl=slug.lower()
    tl=title.lower()
    if "updown-4h-" in sl or "4 hour" in tl or "4hr" in tl:
        return TF.H4
    if "up-or-down-" in sl and ("hourly" in tl or "1 hour" in tl or re.search(r'\d+[ap]m\s+et',tl)):
        return TF.H1
    if any(x in sl for x in ["updown-1h-","updown-hourly-"]):
        return TF.H1
    tf=parse_tf(title)
    if tf and tf in (TF.H1,TF.H4):
        return tf
    return None


def _classify_event_asset(slug:str,title:str)->Optional[Asset]:
    combined=(slug+" "+title).lower()
    for asset,kws in ASSET_KEYWORDS.items():
        if any(kw in combined for kw in kws):
            return asset
    return None


def _is_updown(title:str)->bool:
    tl=title.lower()
    return "up or down" in tl or "updown" in tl or "up/down" in tl


def _parse_event_to_markets(event:Dict,api:PolyAPI)->List[Market]:
    slug=event.get("slug","")
    title=event.get("title","")
    tf=_classify_event_tf(slug,title)
    asset=_classify_event_asset(slug,title)
    if not tf or not asset:
        return []
    if not _is_updown(title):
        return []

    markets_raw=event.get("markets",[])
    results=[]
    for m in markets_raw:
        q=m.get("question","") or title
        mkt_closed=m.get("closed",False)
        mkt_active=m.get("active",True)
        if mkt_closed or not mkt_active:
            continue

        target=parse_target(q)
        direction=parse_dir(q)

        clob_ids_raw=m.get("clobTokenIds","[]")
        prices_raw=m.get("outcomePrices","[]")
        try:
            clob_ids=json.loads(clob_ids_raw) if isinstance(clob_ids_raw,str) else clob_ids_raw
        except:
            clob_ids=[]
        try:
            prices=json.loads(prices_raw) if isinstance(prices_raw,str) else prices_raw
        except:
            prices=[]

        yes_p=float(prices[0]) if len(prices)>0 else 0.5
        no_p=float(prices[1]) if len(prices)>1 else 0.5
        token_yes=clob_ids[0] if len(clob_ids)>0 else ""
        token_no=clob_ids[1] if len(clob_ids)>1 else ""

        try:
            res_t=datetime.fromisoformat(m.get("endDate","").replace("Z","+00:00"))
        except:
            res_t=now()

        if time_to_res(res_t)<60:
            continue

        mk=Market(
            id=m.get("id",""),
            cond_id=m.get("conditionId",""),
            question=q,
            asset=asset,
            tf=tf,
            target=target or 0,
            direction=direction,
            res_time=res_t,
            yes_p=yes_p,
            no_p=no_p,
            vol24=float(m.get("volume24hr",0) or 0),
            liq=float(m.get("liquidity",0) or 0),
            spread=abs(1-yes_p-no_p),
            created=now(),
            slug=slug,
            token_yes=token_yes,
            token_no=token_no,
        )
        results.append(mk)
        api.cache_market(mk)

    return results


class Scanner:
    MIN_VOL={"1hr":30000,"4hr":30000}
    MIN_TIME={"1hr":300,"4hr":900}

    TAG_SLUGS=["crypto"]

    def __init__(self,api:PolyAPI,interval:int=SCAN_INTERVAL):
        self.api=api
        self.interval=interval
        self.markets:Dict[Asset,Dict[TF,List[Market]]]={}
        self.running=False
        self._seen_slugs:Set[str]=set()

    async def start(self):
        self.running=True
        while self.running:
            try:
                await self._scan()
            except Exception as e:
                log.error(f"Scanner error: {e}")
            await asyncio.sleep(self.interval)

    async def stop(self):
        self.running=False

    async def _scan(self):
        self.markets={a:{tf:[] for tf in TF} for a in Asset}
        self._seen_slugs.clear()

        events=[]
        for tag in self.TAG_SLUGS:
            batch=await self.api.fetch_events_by_tag(tag,GAMMA_FETCH_LIMIT)
            events.extend(batch)

        found_h1=0
        found_h4=0
        for ev in events:
            slug=ev.get("slug","")
            if slug in self._seen_slugs:
                continue
            self._seen_slugs.add(slug)

            mkts=_parse_event_to_markets(ev,self.api)
            for m in mkts:
                wt=ASSETS.get(m.asset.value,{}).get("wt",{}).get(m.tf.value,0)
                if wt==0:
                    continue
                if m.vol24<self.MIN_VOL.get(m.tf.value,30000):
                    continue
                if time_to_res(m.res_time)<self.MIN_TIME.get(m.tf.value,300):
                    continue
                self.markets[m.asset][m.tf].append(m)
                if m.tf==TF.H1:found_h1+=1
                else:found_h4+=1

        log.info(f"Scan: {len(events)} events -> {found_h1} H1 + {found_h4} H4 markets")

    def all_markets(self)->List[Market]:
        return [m for a in self.markets.values() for tf in a.values() for m in tf]

    def get(self,a:Asset,tf:TF)->List[Market]:
        return self.markets.get(a,{}).get(tf,[])
