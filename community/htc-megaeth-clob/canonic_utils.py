"""
Utility helpers for Canonic MAOB connector.
"""

from decimal import Decimal
from typing import Tuple


def split_trading_pair(trading_pair: str) -> Tuple[str, str]:
    """Split 'WETH-USDm' → ('WETH', 'USDm')."""
    parts = trading_pair.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid trading pair: {trading_pair}")
    return parts[0], parts[1]


def token_to_wei(amount: Decimal, decimals: int) -> int:
    """Human amount → integer wei."""
    return int(amount * Decimal(10 ** decimals))


def wei_to_token(amount_wei: int, decimals: int) -> Decimal:
    """Integer wei → human Decimal."""
    return Decimal(amount_wei) / Decimal(10 ** decimals)
