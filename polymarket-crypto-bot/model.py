from typing import Dict,List,Optional,Tuple
from collections import deque
from core import Asset,TF,Market,Price,Signal,now,clamp,time_to_res

WEIGHTS={
    TF.M15:{"mom1":0.20,"mom5":0.15,"momv":0.15,"vol":0.10,"flow":0.15,"spread":0.05,"volr":0.10,"time":0.10},
    TF.H1:{"t15":0.20,"t1h":0.15,"str":0.10,"vol":0.10,"drift":0.15,"volr":0.10,"time":0.10,"align":0.10},
    TF.H4:{"t1h":0.20,"t4h":0.20,"div":0.15,"sess":0.15,"volc":0.10,"liq":0.10,"flow":0.10},
    TF.D:{"dir":0.20,"range":0.10,"multi":0.20,"anchor":0.15,"week":0.10,"volr":0.15,"tod":0.10}
}

class Model:
    def __init__(self):
        self._prob_hist:Dict[str,deque]={}
        self._price_hist:Dict[str,deque]={}
    
    def calc(self,m:Market,p:Price,book:Optional[Dict]=None)->Signal:
        tf=m.tf
        base=m.yes_p
        adj=0.0
        conf=0.5
        
        if tf==TF.M15:
            adj,conf=self._m15(m,p,book)
        elif tf==TF.H1:
            adj,conf=self._h1(m,p,book)
        elif tf==TF.H4:
            adj,conf=self._h4(m,p,book)
        else:
            adj,conf=self._daily(m,p,book)
        
        prob=clamp(base+adj,0.05,0.95)
        edge=prob-m.yes_p
        
        return Signal(market=m,model_p=prob,market_p=m.yes_p,edge=edge,conf=conf,ts=now())
    
    def _mom_sig(self,mom:float,d:str,scale:float=0.005)->float:
        s=clamp(mom/scale,-1,1)
        return -s if d=="below" else s
    
    def _m15(self,m:Market,p:Price,book:Optional[Dict])->Tuple[float,float]:
        w=WEIGHTS[TF.M15]
        adj=0.0
        sigs=0
        
        s1=self._mom_sig(p.c1m,m.direction)
        s5=self._mom_sig(p.c5m,m.direction)
        sv=self._mom_sig(p.c5m-p.c1m,m.direction,0.002)
        adj+=s1*w["mom1"]+s5*w["mom5"]+sv*w["momv"]
        if abs(s1)>0.3:sigs+=1
        if abs(s5)>0.3:sigs+=1
        
        if book:
            imb=book.get("bid_sz",1)/max(book.get("ask_sz",1),0.001)
            fs=0.8 if imb>1.5 else(-0.8 if imb<0.67 else 0)
            if m.direction=="below":fs=-fs
            adj+=fs*w["flow"]
            if abs(fs)>0.3:sigs+=1
            if book.get("spread",0.02)<0.016:adj+=0.5*w["spread"]
        
        vr=p.vol2h/0.01 if p.vol2h else 1
        if vr>2:adj-=0.4*w["volr"]
        elif vr<0.5:adj+=0.3*w["volr"]
        
        tr=time_to_res(m.res_time)
        if tr<600:
            ts=0.5 if(p.price>m.target)==(m.direction=="above") else -0.5
            adj+=ts*(1-tr/600)*w["time"]
        
        return adj*0.30,min(1.0,sigs/4+0.3)
    
    def _h1(self,m:Market,p:Price,book:Optional[Dict])->Tuple[float,float]:
        w=WEIGHTS[TF.H1]
        adj=0.0
        sigs=0
        
        s15=self._mom_sig(p.c15m,m.direction,0.01)
        s1h=self._mom_sig(p.c1h,m.direction,0.015)
        adj+=s15*w["t15"]+s1h*w["t1h"]
        if abs(s15)>0.3:sigs+=1
        if abs(s1h)>0.3:sigs+=1
        
        dirs=[1 if p.c5m>0 else -1,1 if p.c15m>0 else -1,1 if p.c1h>0 else -1]
        align=sum(dirs)/3
        ss=abs(align)*self._mom_sig(align,m.direction,1)
        adj+=ss*w["str"]
        
        drift=self._drift(m.id,m.yes_p)
        ds=clamp(drift/0.05,-1,1)
        if m.direction=="below":ds=-ds
        adj+=ds*w["drift"]
        if abs(ds)>0.3:sigs+=1
        
        vr=p.vol2h/0.01 if p.vol2h else 1
        if vr>2:adj-=0.4*w["volr"]
        elif vr<0.5:adj+=0.3*w["volr"]
        
        tr=time_to_res(m.res_time)
        if tr<2400:
            ts=0.5 if(p.price>m.target)==(m.direction=="above") else -0.5
            adj+=ts*(1-tr/2400)*w["time"]
        
        sd=1 if p.c15m>0 else -1
        ld=1 if p.c4h>0 else -1
        if sd==ld:
            adj+=0.6*self._mom_sig(sd,m.direction,1)*w["align"]
        else:
            adj-=0.3*w["align"]
        
        return adj*0.35,min(1.0,sigs/5+0.25)
    
    def _h4(self,m:Market,p:Price,book:Optional[Dict])->Tuple[float,float]:
        w=WEIGHTS[TF.H4]
        adj=0.0
        sigs=0
        
        s1h=self._mom_sig(p.c1h,m.direction,0.015)
        s4h=self._mom_sig(p.c4h,m.direction,0.03)
        adj+=s1h*w["t1h"]+s4h*w["t4h"]
        if abs(s1h)>0.3:sigs+=1
        if abs(s4h)>0.3:sigs+=1
        
        div=self._divergence(m.id,p.price,m.yes_p,m.target,m.direction)
        adj+=div*w["div"]
        if abs(div)>0.3:sigs+=1
        
        h=now().hour
        if 13<=h<=21:adj+=0.3*w["sess"]
        elif 0<=h<=8:adj-=0.1*w["sess"]
        
        if book:
            depth=book.get("bid_sz",0)+book.get("ask_sz",0)
            if depth>10000:adj+=0.3*w["liq"]
            elif depth<2000:adj-=0.3*w["liq"]
            
            imb=book.get("bid_sz",1)/max(book.get("ask_sz",1),0.001)
            if imb>1.3:
                adj+=(0.5 if m.direction=="above" else -0.5)*w["flow"]
            elif imb<0.77:
                adj+=(-0.5 if m.direction=="above" else 0.5)*w["flow"]
        
        return adj*0.35,min(1.0,sigs/5+0.25)
    
    def _daily(self,m:Market,p:Price,book:Optional[Dict])->Tuple[float,float]:
        w=WEIGHTS[TF.D]
        adj=0.0
        sigs=0
        
        dd=1 if p.c1h>0.002 else(-1 if p.c1h<-0.002 else 0)
        ds=0.6*dd
        if m.direction=="below":ds=-ds
        adj+=ds*w["dir"]
        if dd!=0:sigs+=1
        
        s4h=self._mom_sig(p.c4h,m.direction,0.04)
        adj+=s4h*w["multi"]
        if abs(s4h)>0.3:sigs+=1
        
        anchors=[0.25,0.33,0.50,0.67,0.75]
        for a in anchors:
            if abs(m.yes_p-a)<0.03:
                adj+=0.2*w["anchor"]
                break
        
        day=now().weekday()
        if day==0:adj-=0.1*w["week"]
        elif day==4:adj+=0.1*w["week"]
        elif day in[1,2,3]:adj+=0.15*w["week"]
        else:adj-=0.2*w["week"]
        
        vr=p.vol2h/0.01 if p.vol2h else 1
        if vr>2:adj-=0.4*w["volr"]
        elif vr<0.5:adj+=0.3*w["volr"]
        
        tr=time_to_res(m.res_time)
        if tr<14400:
            ts=0.4 if(p.price>m.target)==(m.direction=="above") else -0.4
            adj+=ts*w["tod"]
        
        return adj*0.30,min(1.0,sigs/5+0.20)
    
    def _drift(self,mid:str,prob:float)->float:
        if mid not in self._prob_hist:
            self._prob_hist[mid]=deque(maxlen=30)
        self._prob_hist[mid].append(prob)
        if len(self._prob_hist[mid])<5:return 0
        return prob-self._prob_hist[mid][0]
    
    def _divergence(self,mid:str,price:float,prob:float,target:float,direction:str)->float:
        if mid not in self._price_hist:
            self._price_hist[mid]=deque(maxlen=12)
            self._prob_hist[mid]=deque(maxlen=12)
        self._price_hist[mid].append(price)
        self._prob_hist[mid].append(prob)
        if len(self._price_hist[mid])<3:return 0
        
        old_p=self._price_hist[mid][0]
        pc=(price-old_p)/old_p if old_p>0 else 0
        old_prob=self._prob_hist[mid][0]
        prc=prob-old_prob
        
        exp=pc*3 if direction=="above" else -pc*3
        return clamp((exp-prc)/0.05,-1,1)
