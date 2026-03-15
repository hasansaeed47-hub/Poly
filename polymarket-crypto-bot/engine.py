from typing import Dict,List,Optional,Tuple
from dataclasses import dataclass
from core import Asset,TF,Dir,ExecMode,Market,Signal,Position,now,gen_id,calc_shares,clamp
from config import (TIERS,EDGE_MAKER,EDGE_TAKER,MAX_STAKE,DAILY_BUDGET,
                    MAX_POSITIONS,OPP_W,OPP_TH,CORR,ASSETS)

@dataclass(slots=True)
class Score:
    total:float
    edge:float
    conf:float
    liq:float
    vol:float
    corr:float
    fresh:float
    ok:bool

@dataclass(slots=True)
class SizeRec:
    base:float
    final:float
    tier:str

@dataclass(slots=True)
class ExecDec:
    mode:ExecMode
    price:float

class Scorer:
    def score(self,sig:Signal,positions:Dict[Asset,float]=None,vol:float=1.0,age_sec:float=600)->Score:
        w=OPP_W

        e_s=min(1.0,sig.edge/0.25) if sig.edge>0 else 0
        c_s=min(1.0,sig.conf)
        l_s=min(1.0,sig.market.vol24/100000) if sig.market.vol24>0 else 0

        v_opt=1.5
        v_dist=abs(vol-v_opt)
        v_s=1.0 if v_dist<0.5 else(0.7 if v_dist<1 else(0.4 if v_dist<1.5 else 0.2))

        cor_s=1.0
        if positions:
            if sig.market.asset in positions:cor_s=0.3
            else:
                tc=sum(CORR.get((sig.market.asset,a),CORR.get((a,sig.market.asset),0.5))*(s/100) for a,s in positions.items() if s>0)
                cor_s=max(0,1-tc)

        f_s=1.0 if age_sec<120 else(0.7 if age_sec<300 else(0.4 if age_sec<600 else 0.1))

        total=(e_s*w["edge"]+c_s*w["conf"]+l_s*w["liq"]+v_s*w["vol"]+cor_s*w["corr"]+f_s*w["fresh"])*100

        return Score(total=total,edge=e_s,conf=c_s,liq=l_s,vol=v_s,corr=cor_s,fresh=f_s,ok=total>=OPP_TH)

    def rank(self,sigs:List[Signal],positions:Dict[Asset,float]=None,vol:float=1.0)->List[Tuple[Signal,Score]]:
        scored=[(s,self.score(s,positions,vol)) for s in sigs]
        return sorted(scored,key=lambda x:-x[1].total)


class Sizer:
    def __init__(self,bankroll:float=500):
        self.bankroll=bankroll
        self.daily_used=0.0

    def calc(self,edge:float,corr_adj:float=1.0,daily_used:float=0)->SizeRec:
        tier="min"
        base=5
        for t,(lo,hi,sz) in TIERS.items():
            if lo<=edge<hi:
                tier=t
                base=sz
                break

        adj=base*corr_adj
        remain=DAILY_BUDGET-daily_used
        adj=min(adj,remain,MAX_STAKE)
        final=max(0,round(adj,2))

        return SizeRec(base=base,final=final,tier=tier)

    def add(self,amt:float):
        self.daily_used+=amt

    def reset(self):
        self.daily_used=0

    def can_trade(self)->bool:
        return(DAILY_BUDGET-self.daily_used)>=5


class CorrMgr:
    def check(self,asset:Asset,direction:Dir,positions:List[Position])->Tuple[bool,float]:
        if len(positions)>=MAX_POSITIONS:
            return False,0

        for p in positions:
            if p.market.asset==asset:
                return False,0

        adj=1.0
        for p in positions:
            c=CORR.get((asset,p.market.asset),CORR.get((p.market.asset,asset),0.5))
            if c>0.7 and p.dir==direction:
                adj=min(adj,0.5)

        return True,adj


class ExecDecider:
    def decide(self,m:Market,book:Optional[Dict],edge:float,vol:float,prob_chg:float=0)->ExecDec:
        if vol>2 or m.vol24<50000 or m.spread<0.01 or abs(prob_chg)>0.03:
            price=book["ba"] if book else m.yes_p
            return ExecDec(mode=ExecMode.TAKER,price=price)

        if m.vol24>=100000 and m.spread>=0.02 and edge>=EDGE_MAKER:
            price=(book["mid"]-0.02) if book else(m.yes_p-0.02)
            price=clamp(price,0.01,0.99)
            return ExecDec(mode=ExecMode.MAKER,price=price)

        price=book["ba"] if book else m.yes_p
        return ExecDec(mode=ExecMode.TAKER,price=price)

    def edge_th(self,tf:TF,mode:ExecMode)->float:
        return EDGE_MAKER if mode==ExecMode.MAKER else EDGE_TAKER


class Executor:
    def __init__(self,api):
        self.api=api

    async def enter(self,m:Market,direction:Dir,size:float,price:float,mode:ExecMode)->Optional[Position]:
        side="BUY"
        token_id=m.token_yes if direction==Dir.LONG else m.token_no
        if not token_id:
            token_id=m.id
        shares=calc_shares(size,price)

        if mode==ExecMode.MAKER:
            oid=await self.api.place(token_id,side,price,shares)
        else:
            oid=await self.api.market_order(token_id,side,shares)

        if not oid:return None

        return Position(
            id=gen_id("pos"),market=m,dir=direction,entry_p=price,
            entry_prob=m.yes_p,curr_prob=m.yes_p,size=size,shares=shares,
            entry_t=now(),update_t=now()
        )

    async def exit(self,pos:Position,pct:float=1.0)->bool:
        token_id=pos.market.token_yes if pos.dir==Dir.LONG else pos.market.token_no
        if not token_id:
            token_id=pos.market.id
        shares=pos.shares*pct
        oid=await self.api.market_order(token_id,"SELL",shares)
        return oid is not None
