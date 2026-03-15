from dataclasses import dataclass,field
from datetime import datetime,timezone
from enum import Enum,auto
from typing import Dict,List,Optional,Any
from collections import deque
import hashlib,time

class TF(Enum):
    H1="1hr"
    H4="4hr"

class Asset(Enum):
    BTC="BTC"
    ETH="ETH"
    SOL="SOL"
    XRP="XRP"

class Dir(Enum):
    LONG="LONG"
    SHORT="SHORT"

class Regime(Enum):
    A="inc"
    B="stable"
    C="dec"
    D="div"

class ExecMode(Enum):
    MAKER="maker"
    TAKER="taker"

@dataclass(slots=True)
class Market:
    id:str
    cond_id:str
    question:str
    asset:Asset
    tf:TF
    target:float
    direction:str
    res_time:datetime
    yes_p:float
    no_p:float
    vol24:float
    liq:float
    spread:float
    created:datetime
    slug:str=""
    token_yes:str=""
    token_no:str=""

@dataclass(slots=True)
class Price:
    asset:Asset
    price:float
    ts:datetime
    c1m:float=0
    c5m:float=0
    c15m:float=0
    c1h:float=0
    c4h:float=0
    vol1h:float=0
    vol2h:float=0

@dataclass(slots=True)
class Signal:
    market:Market
    model_p:float
    market_p:float
    edge:float
    conf:float
    ts:datetime

@dataclass
class Position:
    id:str
    market:Market
    dir:Dir
    entry_p:float
    entry_prob:float
    curr_prob:float
    size:float
    shares:float
    entry_t:datetime
    update_t:datetime
    regime:Optional[Regime]=None
    upnl:float=0
    upnl_pct:float=0

@dataclass
class Trade:
    id:str
    market_id:str
    asset:Asset
    tf:TF
    dir:Dir
    entry_p:float
    exit_p:float
    entry_prob:float
    exit_prob:float
    size:float
    shares:float
    entry_t:datetime
    exit_t:datetime
    reason:str
    pnl:float
    pnl_pct:float
    correct:bool

def now()->datetime:
    return datetime.now(timezone.utc)

def ts_ms()->int:
    return int(now().timestamp()*1000)

def gen_id(prefix:str="t")->str:
    return f"{prefix}_{ts_ms()}_{hashlib.sha256(str(ts_ms()).encode()).hexdigest()[:6]}"

def calc_shares(usd:float,price:float)->float:
    return usd/price if 0<price<1 else 0

def calc_pnl(shares:float,entry:float,exit:float)->tuple:
    pnl=shares*(exit-entry)
    pct=(exit-entry)/entry if entry>0 else 0
    return pnl,pct

ASSET_KEYWORDS={
    Asset.BTC:["btc","bitcoin"],
    Asset.ETH:["eth","ethereum"],
    Asset.SOL:["sol","solana"],
    Asset.XRP:["xrp","ripple"],
}

def parse_asset(q:str)->Optional[Asset]:
    ql=q.lower()
    for asset,kws in ASSET_KEYWORDS.items():
        if any(kw in ql for kw in kws):
            return asset
    return None

def parse_tf(q:str)->Optional[TF]:
    ql=q.lower()
    if "4 hour" in ql or "4hr" in ql or "4h" in ql:return TF.H4
    if "1 hour" in ql or "1hr" in ql or "hourly" in ql or "hour" in ql:return TF.H1
    return None

def parse_target(q:str)->Optional[float]:
    import re
    m=re.search(r'\$([0-9,]+(?:\.[0-9]+)?)',q)
    if m:return float(m.group(1).replace(',',''))
    return None

def parse_dir(q:str)->str:
    return "below" if any(x in q.lower() for x in ["below","under","lower","down"]) else "above"

def time_to_res(res_time:datetime)->float:
    return max(0,(res_time-now()).total_seconds())

def clamp(v:float,lo:float,hi:float)->float:
    return max(lo,min(hi,v))

def calc_vol(prices:List[float])->float:
    if len(prices)<2:return 0
    rets=[(prices[i]-prices[i-1])/prices[i-1] for i in range(1,len(prices)) if prices[i-1]>0]
    if not rets:return 0
    m=sum(rets)/len(rets)
    return (sum((r-m)**2 for r in rets)/len(rets))**0.5
