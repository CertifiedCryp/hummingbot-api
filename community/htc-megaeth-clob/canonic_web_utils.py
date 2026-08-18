"""
Web3 / ABI encode-decode + thin RPC client for Canonic MAOB on MegaETH.
Uses eth_abi + aiohttp (no heavy web3 dependency required at import time).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from eth_abi import encode as abi_encode, decode as abi_decode
from eth_utils import function_signature_to_4byte_selector, to_checksum_address

from hummingbot.connector.exchange.canonic import canonic_constants as CONSTANTS

# ── Selectors (computed once) ────────────────────────────────

def _sel(sig: str) -> bytes:
    return function_signature_to_4byte_selector(sig)


SEL_BALANCE_OF = _sel("balanceOf(address)")
SEL_ALLOWANCE = _sel("allowance(address,address)")
SEL_APPROVE = _sel("approve(address,uint256)")
SEL_GET_MID_PRICE = _sel("getMidPrice()")
SEL_GET_DEPTH = _sel("getDepth(uint256)")
SEL_PREVIEW_ORDER = _sel("previewOrder(uint256)")
SEL_CANCEL_ORDER = _sel("cancelOrder(uint256,bool)")
# LiquidityOrder = (uint16 rung, bool isAsk, uint256 amount)
SEL_ADD_LIQUIDITY_BATCH = _sel("addLiquidityBatch((uint16,bool,uint256)[])")


# ── Encode helpers ─────────────────────────────────────

def encode_balance_of(owner: str) -> str:
    data = SEL_BALANCE_OF + abi_encode(["address"], [to_checksum_address(owner)])
    return "0x" + data.hex()


def encode_allowance(owner: str, spender: str) -> str:
    data = SEL_ALLOWANCE + abi_encode(
        ["address", "address"],
        [to_checksum_address(owner), to_checksum_address(spender)],
    )
    return "0x" + data.hex()


def encode_approve(spender: str, amount: int) -> str:
    data = SEL_APPROVE + abi_encode(
        ["address", "uint256"],
        [to_checksum_address(spender), amount],
    )
    return "0x" + data.hex()


def encode_get_mid_price() -> str:
    return "0x" + SEL_GET_MID_PRICE.hex()


def encode_get_depth(max_per_side: int = 20) -> str:
    data = SEL_GET_DEPTH + abi_encode(["uint256"], [max_per_side])
    return "0x" + data.hex()


def encode_preview_order(order_id: int) -> str:
    data = SEL_PREVIEW_ORDER + abi_encode(["uint256"], [order_id])
    return "0x" + data.hex()


def encode_cancel_order(order_id: int, withdraw_after: bool = True) -> str:
    data = SEL_CANCEL_ORDER + abi_encode(["uint256", "bool"], [order_id, withdraw_after])
    return "0x" + data.hex()


def encode_add_liquidity_batch(orders: List[Tuple[int, bool, int]]) -> str:
    """
    orders: list of (rung: uint16, is_ask: bool, amount: uint256)
    """
    data = SEL_ADD_LIQUIDITY_BATCH + abi_encode(
        ["(uint16,bool,uint256)[]"],
        [orders],
    )
    return "0x" + data.hex()


# ── Decode helpers ─────────────────────────────────────

def decode_balance_of(result_hex: str) -> int:
    if not result_hex or result_hex in ("0x", "0x0"):
        return 0
    raw = bytes.fromhex(result_hex[2:] if result_hex.startswith("0x") else result_hex)
    if len(raw) < 32:
        raw = raw.rjust(32, b"\x00")
    (val,) = abi_decode(["uint256"], raw)
    return int(val)


def decode_allowance(result_hex: str) -> int:
    return decode_balance_of(result_hex)


def decode_get_mid_price(result_hex: str) -> Tuple[float, float, int]:
    """
    Returns (price, precision, updated_at).
    Contract may return (uint256 price, uint256 precision, uint64 updatedAt)
    or a single packed mid — we tolerate both.
    """
    raw = bytes.fromhex(result_hex[2:] if result_hex.startswith("0x") else result_hex)
    try:
        if len(raw) >= 96:
            price, precision, updated = abi_decode(["uint256", "uint256", "uint64"], raw[:96])
            prec = float(precision) if precision else 1e18
            return float(price), prec, int(updated)
        if len(raw) >= 32:
            (price,) = abi_decode(["uint256"], raw[:32])
            return float(price), 1e18, 0
    except Exception:
        pass
    return 0.0, 1e18, 0


def decode_preview_order(result_hex: str) -> Tuple[int, int]:
    """
    Returns (unfilled_input, claimable_output).
    """
    raw = bytes.fromhex(result_hex[2:] if result_hex.startswith("0x") else result_hex)
    try:
        if len(raw) >= 64:
            unfilled, claimable = abi_decode(["uint256", "uint256"], raw[:64])
            return int(unfilled), int(claimable)
    except Exception:
        pass
    return 0, 0


def decode_get_depth(result_hex: str) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """
    Best-effort depth decode → (bids, asks) as [(price, size), ...].
    Exact layout depends on MAOB ABI; returns empty on failure.
    """
    # Production: decode via full ABI once confirmed on-chain.
    # MVP: empty book — exchange will fall back to mid-only.
    return [], []


def bps_to_rung(price_offset_bps: float) -> int:
    """
    Map price offset in bps → discrete rung index.
    Simplified: 1 rung ≈ 1 bps (clamp 1..500).
    Production: call getRungs() and nearest-neighbor map.
    """
    rung = int(max(1, min(500, round(price_offset_bps))))
    return rung


# ── RPC client ───────────────────────────────────────

class MegaETHRPCClient:
    """Minimal async JSON-RPC client for MegaETH."""

    def __init__(self, rpc_url: str = CONSTANTS.RPC_URL):
        self._rpc_url = rpc_url
        self._session: Optional[aiohttp.ClientSession] = None
        self._req_id = 0

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _rpc(self, method: str, params: list) -> Any:
        await self._ensure_session()
        self._req_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": method,
            "params": params,
        }
        async with self._session.post(self._rpc_url, json=payload) as resp:
            body = await resp.json()
            if "error" in body:
                raise RuntimeError(f"RPC error: {body['error']}")
            return body.get("result")

    async def eth_call(self, to: str, data: str, block: str = "latest") -> str:
        return await self._rpc("eth_call", [{"to": to, "data": data}, block])

    async def batch_call(self, calls: List[Dict[str, str]]) -> List[Dict]:
        """Sequential eth_call batch (public RPC friendly)."""
        results = []
        for c in calls:
            try:
                r = await self.eth_call(c["to"], c["data"])
                results.append({"result": r})
            except Exception as e:
                results.append({"error": str(e), "result": "0x0"})
        return results

    async def get_gas_price(self) -> int:
        hex_price = await self._rpc("eth_gasPrice", [])
        return int(hex_price, 16)

    async def get_transaction_count(self, address: str) -> int:
        hex_n = await self._rpc("eth_getTransactionCount", [address, "pending"])
        return int(hex_n, 16)

    async def send_raw_transaction(self, raw_hex: str) -> str:
        return await self._rpc("eth_sendRawTransaction", [raw_hex])

    async def wait_for_transaction_receipt(
        self, tx_hash: str, timeout: float = 30.0, poll: float = 0.5
    ) -> Dict:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            receipt = await self._rpc("eth_getTransactionReceipt", [tx_hash])
            if receipt is not None:
                return receipt
            await asyncio.sleep(poll)
        raise TimeoutError(f"Receipt timeout for {tx_hash}")

    async def get_block_number(self) -> int:
        hex_n = await self._rpc("eth_blockNumber", [])
        return int(hex_n, 16)
