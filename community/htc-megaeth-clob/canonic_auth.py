"""
Wallet auth for Canonic MAOB (EOA private key signing).
"""

from typing import Any, Dict, Optional

from eth_account import Account
from eth_account.signers.local import LocalAccount
from eth_account.messages import encode_defunct


class CanonicAuth:
    """
    Signs transactions with a MegaETH EOA private key.
    No API keys — pure on-chain auth.
    """

    def __init__(self, private_key: str):
        if private_key and not private_key.startswith("0x"):
            private_key = "0x" + private_key
        self._private_key = private_key
        self._account: Optional[LocalAccount] = None
        if private_key:
            self._account = Account.from_key(private_key)

    @property
    def address(self) -> Optional[str]:
        return self._account.address if self._account else None

    def build_transaction(
        self,
        to: str,
        data: str,
        gas: int,
        gas_price: int,
        nonce: int,
        value: int = 0,
        chain_id: int = 4326,
    ) -> Dict[str, Any]:
        return {
            "to": to,
            "data": data,
            "gas": gas,
            "gasPrice": gas_price,
            "nonce": nonce,
            "value": value,
            "chainId": chain_id,
        }

    def sign_transaction(self, tx: Dict[str, Any]) -> bytes:
        if not self._account:
            raise RuntimeError("No private key configured")
        signed = self._account.sign_transaction(tx)
        # eth-account returns SignedTransaction with rawTransaction or raw_transaction
        raw = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
        if raw is None:
            raise RuntimeError("Could not extract raw transaction bytes")
        return bytes(raw)

    def sign_message(self, message: str) -> str:
        if not self._account:
            raise RuntimeError("No private key configured")
        msg = encode_defunct(text=message)
        signed = self._account.sign_message(msg)
        return signed.signature.hex()
