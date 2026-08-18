"""
Constants for Canonic MAOB connector on MegaETH.
"""

import sys
from decimal import Decimal

from hummingbot.core.api_throttler.data_types import RateLimit

# ── Identity ──────────────────────────────────────────────
DEFAULT_DOMAIN = "mainnet"
HBOT_ORDER_ID_PREFIX = "CNO"
MAX_ORDER_ID_LEN = 32

# ── Chain ───────────────────────────────────────────────
CHAIN_ID = 4326
RPC_URL = "https://mainnet.megaeth.com/rpc"
WS_URL = "wss://mainnet.megaeth.com/ws"
# Fallback realtime endpoints
WS_URL_FALLBACKS = [
    "wss://carrot.megaeth.com/ws",
    "wss://rpc.megaeth.com/ws",
]

# ── Contracts (MegaETH mainnet) ──────────────────────────────
# WETH / USDm from official MegaETH docs
WETH_CONTRACT = "0x4200000000000000000000000000000000000006"
USDM_CONTRACT = "0xFAfDdbb3FC7688494971a79cc65DCa3EF82079E7"
# MAOB WETH-USDm orderbook (DefiLlama / Canonic)
CLOB_CONTRACT = "0x23469683e25b780DFDC11410a8e83c923caDF125"

BASE_CURRENCY = "WETH"
QUOTE_CURRENCY = "USDm"
BASE_DECIMALS = 18
QUOTE_DECIMALS = 18

SUPPORTED_TRADING_PAIRS = ["WETH-USDm"]

# ── Fees ────────────────────────────────────────────
# Canonic: 0 maker / ~0.03% taker (300 PPM)
MAKER_FEE_PERCENT = Decimal("0")
TAKER_FEE_PERCENT = Decimal("0.0003")  # 0.03%
TAKER_FEE_PPM = 300

# ── Gas limits ───────────────────────────────────────
APPROVE_GAS_LIMIT = 80_000
PLACE_ORDER_GAS_LIMIT = 350_000
CANCEL_GAS_LIMIT = 200_000

# ── Trading rules defaults ───────────────────────────────
MIN_QUOTE_AMOUNT = Decimal("10")  # min ~10 USDm notional

# ── Event topics (partial match used in receipt parsing) ─────────────
# OrderPlaced(uint256 orderId, ...) — first 4 bytes used for heuristic match
ORDER_PLACED_TOPIC = "0x" + "00" * 32  # placeholder; tighten with real sig when known

# ── Rate limits (conservative public RPC) ────────────────────
NO_LIMIT = sys.maxsize
RATE_LIMITS = [
    RateLimit(limit_id="RPC", limit=25, time_interval=1),
    RateLimit(limit_id="PlaceOrder", limit=5, time_interval=1),
    RateLimit(limit_id="CancelOrder", limit=5, time_interval=1),
]
