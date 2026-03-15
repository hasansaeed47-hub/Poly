import asyncio,signal,logging,os
from typing import Dict,List
from config import get_creds,ASSETS,EDGE_TAKER
from core import Asset,TF,Dir,now
from data import PriceFeed,PolyAPI,Scanner
from model import Model
from engine import Scorer,Sizer,CorrMgr,ExecDecider,Executor
from manager import PosMgr,RegimeClass,ReEvalSched,ExitHandler
from tracking import DB,Calibration,RetrainMon

logging.basicConfig(level=logging.INFO,format='%(asctime)s|%(levelname)s|%(message)s')
log=logging.getLogger(__name__)

class Bot:
    def __init__(self):
        self.running=False
        c=get_creds()
        self.paper=c.paper

        self.pf=PriceFeed()
        self.api=PolyAPI()
        self.scanner=Scanner(self.api)

        self.model=Model()
        self.scorer=Scorer()
        self.sizer=Sizer()
        self.corr=CorrMgr()
        self.exec_dec=ExecDecider()
        self.executor=Executor(self.api)

        self.pm=PosMgr()
        self.rc=RegimeClass()
        self.reeval=ReEvalSched()
        self.exit_h=ExitHandler(self.rc)

        self.db=DB()
        self.calib=None
        self.retrain=None

    async def init(self):
        log.info(f"=== POLYMARKET BOT H1+H4 ({'PAPER' if self.paper else 'LIVE'}) ===")
        await self.api.init()
        self.db.init()
        self.calib=Calibration(self.db)
        self.retrain=RetrainMon(self.db,self.calib)

        self.pm.on_close=self._on_close
        self.reeval.on_reeval=self._on_reeval
        self.exit_h.execute=self._exec_exit

        log.info("Init complete — scanning Gamma API for hourly + 4-hour crypto markets")

    async def start(self):
        self.running=True
        asyncio.create_task(self.pf.start())
        asyncio.create_task(self.scanner.start())
        await self.reeval.start()
        await self._main()

    async def stop(self):
        log.info("Stopping...")
        self.running=False
        await self.pf.stop()
        await self.scanner.stop()
        await self.reeval.stop()
        await self.api.close()
        self.db.close()
        log.info("Stopped")

    async def _main(self):
        while self.running:
            try:
                if not self.pm.can_trade():
                    await asyncio.sleep(60)
                    continue

                if self.retrain.stop_trading():
                    log.warning("Retrain needed - pausing")
                    await asyncio.sleep(300)
                    continue

                await self._scan()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Main error: {e}")
                await asyncio.sleep(30)

    async def _scan(self):
        markets=self.scanner.all_markets()
        if not markets:return

        async def proc_market(m):
            p=self.pf.get(m.asset)
            if not p:return None
            wt=ASSETS.get(m.asset.value,{}).get("wt",{}).get(m.tf.value,0)
            if wt==0:return None

            book=await self.api.get_book(m.token_yes or m.id)
            sig=self.model.calc(m,p,book)

            if sig.edge<EDGE_TAKER:return None
            return sig

        results=await asyncio.gather(*[proc_market(m) for m in markets],return_exceptions=True)
        signals=[r for r in results if r and not isinstance(r,Exception)]

        if not signals:return

        positions={p.market.asset:p.size for p in self.pm.all()}
        vol=self.pf.vol(Asset.BTC)
        ranked=self.scorer.rank(signals,positions,vol)

        for sig,score in ranked[:3]:
            if not score.ok:continue
            await self._process(sig,score)

    async def _process(self,sig,score):
        m=sig.market
        direction=Dir.LONG if sig.edge>0 else Dir.SHORT

        ok,adj=self.corr.check(m.asset,direction,self.pm.all())
        if not ok:return

        sz=self.sizer.calc(sig.edge,adj,self.pm.exposure())
        if sz.final<5:return

        book=await self.api.get_book(m.token_yes or m.id)
        ed=self.exec_dec.decide(m,book,sig.edge,self.pf.vol(m.asset))

        log.info(f"ENTRY: {m.asset.value} {m.tf.value} | Edge:{sig.edge:.1%} | ${sz.final} | {ed.mode.value} | {m.slug}")

        pos=await self.executor.enter(m,direction,sz.final,ed.price,ed.mode)
        if pos:
            self.pm.add(pos)
            self.reeval.register(pos)
            self.sizer.add(sz.final)

    async def _on_reeval(self,pid:str,tf:TF):
        p=self.pm.get(pid)
        if not p:
            self.reeval.unregister(pid,tf)
            return

        await self.api.refresh(p.market.id)
        prob=p.market.yes_p
        self.pm.update(pid,prob)

        regime=self.rc.classify(p,prob)
        p.regime=regime

        price=prob if p.dir==Dir.LONG else 1-prob
        action=await self.exit_h.eval(p,prob,price)

        if action and action["act"]!="hold":
            await self.exit_h.handle(p,action)

    async def _exec_exit(self,p,pct:float,reason:str):
        ok=await self.executor.exit(p,pct)
        if not ok:return None

        exit_p=p.curr_prob if p.dir==Dir.LONG else 1-p.curr_prob

        if pct>=1.0:
            t=await self.pm.close(p.id,exit_p,reason,p.upnl>0)
            if t:
                self.db.log(t)
                self.reeval.unregister(p.id,p.market.tf)
            return t
        return None

    async def _on_close(self,t):
        log.info(f"CLOSED: {t.asset.value} {t.tf.value} | PnL:${t.pnl:+.2f} | {t.reason}")


async def main():
    os.makedirs("data",exist_ok=True)
    bot=Bot()
    loop=asyncio.get_event_loop()
    for s in(signal.SIGINT,signal.SIGTERM):
        loop.add_signal_handler(s,lambda:asyncio.create_task(bot.stop()))

    try:
        await bot.init()
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()

if __name__=="__main__":
    asyncio.run(main())
