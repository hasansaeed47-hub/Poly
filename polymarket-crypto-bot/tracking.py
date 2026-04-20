import sqlite3
from typing import Dict,List,Optional
from pathlib import Path
from core import Asset,TF,Trade,now
from config import CALIB_BINS,CALIB_TOL,RETRAIN_IMM,RETRAIN_URG

class DB:
    def __init__(self,path:str="data/trades.db"):
        self.path=path
        self.conn:Optional[sqlite3.Connection]=None
    
    def init(self):
        Path(self.path).parent.mkdir(parents=True,exist_ok=True)
        self.conn=sqlite3.connect(self.path)
        self.conn.row_factory=sqlite3.Row
        self.conn.execute("""CREATE TABLE IF NOT EXISTS trades(
            id TEXT PRIMARY KEY,market_id TEXT,asset TEXT,tf TEXT,dir TEXT,
            entry_p REAL,exit_p REAL,entry_prob REAL,exit_prob REAL,
            size REAL,shares REAL,entry_t TEXT,exit_t TEXT,reason TEXT,
            pnl REAL,pnl_pct REAL,correct INTEGER,created TEXT)""")
        self.conn.commit()
    
    def log(self,t:Trade):
        self.conn.execute("""INSERT INTO trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (t.id,t.market_id,t.asset.value,t.tf.value,t.dir.value,t.entry_p,t.exit_p,
             t.entry_prob,t.exit_prob,t.size,t.shares,t.entry_t.isoformat(),
             t.exit_t.isoformat(),t.reason,t.pnl,t.pnl_pct,1 if t.correct else 0,now().isoformat()))
        self.conn.commit()
    
    def stats(self,hours:int=48,asset:Asset=None,tf:TF=None)->Dict:
        from datetime import datetime,timedelta,timezone
        cutoff=(now()-timedelta(hours=hours)).isoformat()
        q="SELECT COUNT(*) as n,SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END) as w,SUM(pnl) as pnl,AVG(exit_p-entry_p) as edge FROM trades WHERE entry_t>=?"
        p=[cutoff]
        if asset:q+=" AND asset=?";p.append(asset.value)
        if tf:q+=" AND tf=?";p.append(tf.value)
        r=self.conn.execute(q,p).fetchone()
        n=r["n"] or 0
        w=r["w"] or 0
        return{"n":n,"w":w,"wr":w/n if n>0 else 0,"pnl":r["pnl"] or 0,"edge":r["edge"] or 0}
    
    def calib(self,lo:float,hi:float,asset:Asset=None,tf:TF=None)->Dict:
        q="SELECT COUNT(*) as n,SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END) as w FROM trades WHERE entry_prob>=? AND entry_prob<?"
        p=[lo,hi]
        if asset:q+=" AND asset=?";p.append(asset.value)
        if tf:q+=" AND tf=?";p.append(tf.value)
        r=self.conn.execute(q,p).fetchone()
        n=r["n"] or 0
        w=r["w"] or 0
        return{"lo":lo,"hi":hi,"n":n,"w":w,"actual":w/n if n>0 else 0,"expected":(lo+hi)/2}
    
    def close(self):
        if self.conn:self.conn.close()


class Calibration:
    def __init__(self,db:DB):
        self.db=db
    
    def report(self,asset:Asset=None,tf:TF=None)->List[Dict]:
        return[{**self.db.calib(lo,hi,asset,tf),"ok":abs(self.db.calib(lo,hi,asset,tf)["actual"]-self.db.calib(lo,hi,asset,tf)["expected"])<=CALIB_TOL} for lo,hi in CALIB_BINS]
    
    def check(self,asset:Asset=None,tf:TF=None)->Dict:
        rpt=self.report(asset,tf)
        mis=[r for r in rpt if r["n"]>=10 and not r["ok"]]
        over=[r for r in mis if r["actual"]<r["expected"]]
        under=[r for r in mis if r["actual"]>r["expected"]]
        return{"ok":len(mis)==0,"mis":len(mis),"over":len(over),"under":len(under)}
    
    def modifier(self,prob:float,asset:Asset=None,tf:TF=None)->float:
        for lo,hi in CALIB_BINS:
            if lo<=prob<hi:
                d=self.db.calib(lo,hi,asset,tf)
                if d["n"]<10:return 1.0
                diff=d["actual"]-d["expected"]
                if abs(diff)<=CALIB_TOL:return 1.0
                return max(0.5,1-diff) if diff<0 else min(1.5,1+diff)
        return 1.0


class RetrainMon:
    def __init__(self,db:DB,calib:Calibration):
        self.db=db
        self.calib=calib
    
    def check(self)->Dict:
        s=self.db.stats(48)
        c=self.calib.check()
        level="none"
        reasons=[]
        
        if s["wr"]<RETRAIN_IMM["wr"]:
            level="immediate"
            reasons.append(f"WR {s['wr']:.1%}<{RETRAIN_IMM['wr']:.1%}")
        if s["edge"]<RETRAIN_IMM["edge"]:
            level="immediate"
            reasons.append(f"Edge {s['edge']:.1%}<{RETRAIN_IMM['edge']:.1%}")
        
        if level!="immediate":
            if s["wr"]<RETRAIN_URG["wr"]:
                level="urgent"
                reasons.append(f"WR {s['wr']:.1%}<{RETRAIN_URG['wr']:.1%}")
            if s["edge"]<RETRAIN_URG["edge"]:
                level="urgent"
                reasons.append(f"Edge {s['edge']:.1%}<{RETRAIN_URG['edge']:.1%}")
            if not c["ok"]:
                level="urgent"
                reasons.append(f"{c['mis']} bins miscalibrated")
        
        return{"level":level,"reasons":reasons,"stats":s,"calib":c}
    
    def stop_trading(self)->bool:
        return self.check()["level"]=="immediate"
