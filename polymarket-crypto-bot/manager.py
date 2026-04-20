import asyncio
from typing import Dict,List,Optional,Callable,Set
from core import Asset,TF,Dir,Regime,Position,Trade,now,gen_id,calc_pnl
from config import REGIME_TH,REEVAL_INT,TP_TH,DAILY_LOSS_LIMIT,WEEKLY_LOSS_LIMIT

class PosMgr:
    def __init__(self):
        self.positions:Dict[str,Position]={}
        self.trades:List[Trade]=[]
        self.pnl_day=0.0
        self.pnl_week=0.0
        self.day_stop=False
        self.week_stop=False
        self.on_close:Optional[Callable]=None
        self.on_limit:Optional[Callable]=None
    
    def add(self,p:Position):
        self.positions[p.id]=p
    
    def get(self,pid:str)->Optional[Position]:
        return self.positions.get(pid)
    
    def all(self)->List[Position]:
        return list(self.positions.values())
    
    def by_asset(self,a:Asset)->List[Position]:
        return[p for p in self.positions.values() if p.market.asset==a]
    
    def update(self,pid:str,prob:float,regime:Optional[Regime]=None):
        p=self.positions.get(pid)
        if not p:return
        p.curr_prob=prob
        p.update_t=now()
        if regime:p.regime=regime
        exit_p=prob if p.dir==Dir.LONG else 1-prob
        p.upnl,p.upnl_pct=calc_pnl(p.shares,p.entry_p,exit_p)
    
    async def close(self,pid:str,exit_p:float,reason:str,correct:bool=False)->Optional[Trade]:
        p=self.positions.get(pid)
        if not p:return None
        
        pnl,pnl_pct=calc_pnl(p.shares,p.entry_p,exit_p)
        
        t=Trade(
            id=gen_id("t"),market_id=p.market.id,asset=p.market.asset,tf=p.market.tf,
            dir=p.dir,entry_p=p.entry_p,exit_p=exit_p,entry_prob=p.entry_prob,
            exit_prob=p.curr_prob,size=p.size,shares=p.shares,entry_t=p.entry_t,
            exit_t=now(),reason=reason,pnl=pnl,pnl_pct=pnl_pct,correct=correct
        )
        
        self.pnl_day+=pnl
        self.pnl_week+=pnl
        self._check_limits()
        
        del self.positions[pid]
        self.trades.append(t)
        
        if self.on_close:await self.on_close(t)
        return t
    
    def _check_limits(self):
        if self.pnl_day<=DAILY_LOSS_LIMIT and not self.day_stop:
            self.day_stop=True
            if self.on_limit:asyncio.create_task(self.on_limit("daily",self.pnl_day))
        if self.pnl_week<=WEEKLY_LOSS_LIMIT and not self.week_stop:
            self.week_stop=True
            if self.on_limit:asyncio.create_task(self.on_limit("weekly",self.pnl_week))
    
    def can_trade(self)->bool:
        return not self.day_stop and not self.week_stop
    
    def exposure(self)->float:
        return sum(p.size for p in self.positions.values())
    
    def reset_day(self):
        self.pnl_day=0
        self.day_stop=False
    
    def reset_week(self):
        self.pnl_week=0
        self.week_stop=False


class RegimeClass:
    def classify(self,p:Position,prob:float)->Regime:
        th=REGIME_TH.get(p.market.tf.value,REGIME_TH["1hr"])
        chg=prob-p.entry_prob
        
        if prob<th["div"]:return Regime.D
        if chg>=th["inc"]:return Regime.A
        if chg<=th["dec"]:return Regime.C
        return Regime.B
    
    def action(self,regime:Regime,p:Position,curr_p:float)->Dict:
        pnl_pct=(curr_p-p.entry_p)/p.entry_p if p.entry_p>0 else 0
        
        if regime==Regime.A:
            return{"act":"hold","pct":0}
        elif regime==Regime.B:
            if pnl_pct>=TP_TH:
                return{"act":"tp","pct":1.0}
            return{"act":"hold","pct":0}
        elif regime==Regime.C:
            loss=abs(pnl_pct) if pnl_pct<0 else 0
            if loss<0.05:
                return{"act":"partial","pct":0.5}
            return{"act":"exit","pct":1.0}
        else:
            return{"act":"exit","pct":1.0}


class ReEvalSched:
    def __init__(self):
        self.running=False
        self.tasks:Dict[TF,asyncio.Task]={}
        self.pos_ids:Dict[TF,Set[str]]={tf:set() for tf in TF}
        self.on_reeval:Optional[Callable]=None
    
    async def start(self):
        self.running=True
        for tf in TF:
            self.tasks[tf]=asyncio.create_task(self._loop(tf))
    
    async def stop(self):
        self.running=False
        for t in self.tasks.values():t.cancel()
        await asyncio.gather(*self.tasks.values(),return_exceptions=True)
    
    def register(self,p:Position):
        self.pos_ids[p.market.tf].add(p.id)
    
    def unregister(self,pid:str,tf:TF):
        self.pos_ids[tf].discard(pid)
    
    async def _loop(self,tf:TF):
        interval=REEVAL_INT[tf.value]
        while self.running:
            await asyncio.sleep(interval)
            if not self.running:break
            pids=list(self.pos_ids[tf])
            if pids and self.on_reeval:
                await asyncio.gather(*[self.on_reeval(pid,tf) for pid in pids],return_exceptions=True)


class ExitHandler:
    def __init__(self,regime_cls:RegimeClass):
        self.rc=regime_cls
        self.execute:Optional[Callable]=None
        self.partials:Dict[str,float]={}
    
    async def eval(self,p:Position,prob:float,price:float)->Optional[Dict]:
        regime=self.rc.classify(p,prob)
        return{"regime":regime,**self.rc.action(regime,p,price)}
    
    async def handle(self,p:Position,action:Dict)->Optional[Trade]:
        if action["act"]=="hold":return None
        
        pct=action["pct"]
        already=self.partials.get(p.id,0)
        remain=1-already
        pct=min(pct,remain)
        
        if pct<=0:return None
        
        if self.execute:
            result=await self.execute(p,pct,action["act"])
            if result and action["act"]=="partial":
                self.partials[p.id]=already+pct
            elif result:
                self.partials.pop(p.id,None)
            return result
        return None
    
    def clear(self,pid:str):
        self.partials.pop(pid,None)
