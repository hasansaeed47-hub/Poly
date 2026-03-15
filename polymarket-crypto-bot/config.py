import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()

BANKROLL=500
MAX_STAKE=10
DAILY_BUDGET=100
MAX_POSITIONS=2
DAILY_LOSS_LIMIT=-50
WEEKLY_LOSS_LIMIT=-100

TIERS={"min":(0.03,0.05,5),"std":(0.05,0.08,7),"strong":(0.08,0.12,10),"exc":(0.12,1.0,10)}
EDGE_MAKER=0.03
EDGE_TAKER=0.05

REGIME_TH={
    "1hr":{"inc":0.03,"stable":0.03,"dec":-0.05,"div":0.42},
    "4hr":{"inc":0.02,"stable":0.03,"dec":-0.07,"div":0.38},
}
REEVAL_INT={"1hr":120,"4hr":300}
TP_TH=0.10

CALIB_BINS=[(0.50,0.55),(0.55,0.60),(0.60,0.70),(0.70,0.80),(0.80,0.90),(0.90,1.00)]
CALIB_TOL=0.05
RETRAIN_IMM={"wr":0.45,"edge":0.01}
RETRAIN_URG={"wr":0.48,"edge":0.02}

ASSETS={
    "BTC":{"sym":"BTCUSDT","wt":{"1hr":0.50,"4hr":0.50}},
    "ETH":{"sym":"ETHUSDT","wt":{"1hr":0.55,"4hr":0.45}},
    "SOL":{"sym":"SOLUSDT","wt":{"1hr":0.40,"4hr":0.60}},
    "XRP":{"sym":"XRPUSDT","wt":{"1hr":0.60,"4hr":0.40}},
}
CORR={("BTC","ETH"):0.85,("BTC","SOL"):0.65,("BTC","XRP"):0.55,("ETH","SOL"):0.70,("ETH","XRP"):0.60,("SOL","XRP"):0.50}

OPP_W={"edge":0.25,"conf":0.20,"liq":0.15,"vol":0.15,"corr":0.10,"fresh":0.10}
OPP_TH=40

GAMMA_API="https://gamma-api.polymarket.com"
CLOB_API="https://clob.polymarket.com"

SCAN_INTERVAL=45
GAMMA_FETCH_LIMIT=100

@dataclass
class Creds:
    pm_key:str
    pm_secret:str
    pm_pass:str
    poly_key:str
    paper:bool

def get_creds()->Creds:
    return Creds(
        pm_key=os.getenv("POLYMARKET_API_KEY",""),
        pm_secret=os.getenv("POLYMARKET_SECRET",""),
        pm_pass=os.getenv("POLYMARKET_PASSPHRASE",""),
        poly_key=os.getenv("POLYGON_API_KEY",""),
        paper=os.getenv("PAPER_TRADE","true").lower()=="true"
    )
