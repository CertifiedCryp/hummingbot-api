"""Canonic MAOB DEX Exchange Connector"""

import asyncio
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.canonic import canonic_constants as CONSTANTS
from hummingbot.connector.exchange.canonic import canonic_utils
from hummingbot.connector.exchange.canonic import canonic_web_utils as web_utils
from hummingbot.connector.exchange.canonic.canonic_api_order_book_data_source import CanonicAPIOrderBookDataSource
from hummingbot.connector.exchange.canonic.canonic_api_user_stream_data_source import CanonicAPIUserStreamDataSource
from hummingbot.connector.exchange.canonic.canonic_auth import CanonicAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


@dataclass
class ActiveOrder:
    """Represents an active order on MAOB"""
    order_id: int
    client_order_id: str
    rung: int
    is_ask: bool
    amount: int  # Raw amount in wei
    placed_at: datetime


class CanonicExchange(ExchangePyBase):
    """
    Canonic MAOB DEX Exchange Connector.

    Connects to Canonic's MAOB (Mid-Auction Order Book) on MegaETH for
    direct on-chain trading.

    Key features:
    - Direct web3 execution (no API intermediary)
    - Rung-based order placement
    - Atomic batch operations (addLiquidityBatch, claimAndRequote)
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    def __init__(
        self,
        canonic_private_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._private_key = canonic_private_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or CONSTANTS.SUPPORTED_TRADING_PAIRS

        # Auth (wallet-based)
        self._canonic_auth = CanonicAuth(canonic_private_key)

        # RPC client for direct contract calls
        self._rpc_client = web_utils.MegaETHRPCClient()

        # Active orders tracking
        self._active_orders: Dict[str, ActiveOrder] = {}  # client_order_id -> ActiveOrder

        # Last known mid price
        self._last_mid_price: float = 0.0

        # Token approvals status
        self._tokens_approved: bool = False

        super().__init__(balance_asset_limit)

    @staticmethod
    def canonic_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(canonic_type: str) -> OrderType:
        return OrderType[canonic_type]

    @property
    def authenticator(self):
        return self._canonic_auth

    @property
    def name(self) -> str:
        return "canonic"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return ""  # Not applicable for on-chain DEX

    @property
    def trading_pairs_request_path(self):
        return ""  # Not applicable

    @property
    def check_network_request_path(self):
        return ""  # Not applicable

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def web_utils(self):
        return web_utils

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True  # On-chain cancellation is synchronous

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        # MAOB supports limit orders via rung placement
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def start_network(self):
        await super().start_network()
        await self._update_trading_rules()
        if self.is_trading_required:
            await self._check_and_approve_tokens()

    async def stop_network(self):
        await super().stop_network()
        await self._rpc_client.close()

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        # Not using traditional web assistants for on-chain connector
        return None

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CanonicAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CanonicAPIUserStreamDataSource(
            auth=self._canonic_auth,
            domain=self.domain,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        trade_base_fee = build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            order_side=order_side,
            order_type=order_type,
            amount=amount,
            price=price,
            base_currency=base_currency.upper(),
            quote_currency=quote_currency.upper(),
        )
        return trade_base_fee

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    async def _format_trading_rules(self, exchange_info_dict: List) -> List[TradingRule]:
        """Create trading rules for supported pairs."""
        rules = []
        for trading_pair in self._trading_pairs:
            base, quote = canonic_utils.split_trading_pair(trading_pair)
            rules.append(
                TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal("0.001"),  # Min ~10 USDm / price
                    min_price_increment=Decimal("0.01"),
                    min_base_amount_increment=Decimal("0.0001"),
                    min_notional_size=Decimal(str(CONSTANTS.MIN_QUOTE_AMOUNT)),
                )
            )
        return rules

    async def _update_trading_rules(self):
        """Update trading rules."""
        self._trading_rules = {
            rule.trading_pair: rule
            for rule in await self._format_trading_rules([])
        }

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List):
        mapping = bidict()
        for trading_pair in self._trading_pairs:
            base, quote = canonic_utils.split_trading_pair(trading_pair)
            mapping[trading_pair] = combine_to_hb_trading_pair(
                base=base.upper(), quote=quote.upper()
            )
        self._set_trading_pair_symbol_map(mapping)

    async def _check_and_approve_tokens(self) -> bool:
        """Check token allowances and approve if needed."""
        if self._tokens_approved:
            return True

        wallet = self._canonic_auth.address
        if not wallet:
            return False

        try:
            max_uint256 = 2**256 - 1

            # Check WETH allowance
            weth_allowance_data = web_utils.encode_allowance(wallet, CONSTANTS.CLOB_CONTRACT)
            usdm_allowance_data = web_utils.encode_allowance(wallet, CONSTANTS.CLOB_CONTRACT)

            calls = [
                {"to": CONSTANTS.WETH_CONTRACT, "data": weth_allowance_data},
                {"to": CONSTANTS.USDM_CONTRACT, "data": usdm_allowance_data},
            ]

            results = await self._rpc_client.batch_call(calls)

            weth_allowance = web_utils.decode_allowance(results[0].get("result", "0x0")) if results else 0
            usdm_allowance = web_utils.decode_allowance(results[1].get("result", "0x0")) if len(results) > 1 else 0

            self.logger().info(f"Current allowances - WETH: {weth_allowance / 1e18:.4f}, USDm: {usdm_allowance / 1e18:.4f}")

            # Approve WETH if needed
            if weth_allowance < int(1e18):  # Less than 1 WETH approved
                self.logger().info("Approving WETH...")
                await self._approve_token(CONSTANTS.WETH_CONTRACT, max_uint256)

            # Approve USDm if needed
            if usdm_allowance < int(100 * 1e18):  # Less than 100 USDm approved
                self.logger().info("Approving USDm...")
                await self._approve_token(CONSTANTS.USDM_CONTRACT, max_uint256)

            self._tokens_approved = True
            return True

        except Exception as e:
            self.logger().error(f"Token approval error: {e}")
            return False

    async def _approve_token(self, token_address: str, amount: int):
        """Approve token spending."""
        wallet = self._canonic_auth.address
        approve_data = web_utils.encode_approve(CONSTANTS.CLOB_CONTRACT, amount)

        gas_price = await self._rpc_client.get_gas_price()
        nonce = await self._rpc_client.get_transaction_count(wallet)

        tx = self._canonic_auth.build_transaction(
            to=token_address,
            data=approve_data,
            gas=CONSTANTS.APPROVE_GAS_LIMIT,
            gas_price=gas_price,
            nonce=nonce,
        )

        signed_tx = self._canonic_auth.sign_transaction(tx)
        tx_hash = await self._rpc_client.send_raw_transaction("0x" + signed_tx.hex())
        self.logger().info(f"Approval tx: {tx_hash}")

        receipt = await self._rpc_client.wait_for_transaction_receipt(tx_hash)
        if int(receipt.get("status", "0x0"), 16) != 1:
            raise Exception(f"Approval failed: {receipt}")

        return tx_hash

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        """
        Place an order on MAOB.

        Uses addLiquidityBatch for single order placement.
        """
        # Ensure tokens are approved
        if not await self._check_and_approve_tokens():
            raise Exception("Token approval failed")

        # Get current mid price
        mid_price = await self._get_mid_price()
        if not mid_price:
            raise Exception("Could not fetch mid price")

        # Calculate rung from price
        is_ask = trade_type == TradeType.SELL

        if is_ask:
            # Selling: price should be above mid
            price_offset_bps = ((float(price) - mid_price) / mid_price) * 10000
        else:
            # Buying: price should be below mid
            price_offset_bps = ((mid_price - float(price)) / mid_price) * 10000

        price_offset_bps = max(1, price_offset_bps)  # Minimum 0.1 bps
        rung = web_utils.bps_to_rung(price_offset_bps)

        # Convert amount to wei
        base, quote = canonic_utils.split_trading_pair(trading_pair)

        if is_ask:
            # For asks, amount is in base (WETH)
            amount_wei = canonic_utils.token_to_wei(amount, CONSTANTS.BASE_DECIMALS)
        else:
            # For bids, amount is in quote (USDm) = base_amount * price
            amount_usdm = amount * price
            amount_wei = canonic_utils.token_to_wei(amount_usdm, CONSTANTS.QUOTE_DECIMALS)

        # Build order tuple: (rung, isAsk, amount)
        order = (rung, is_ask, amount_wei)

        # Encode addLiquidityBatch call
        tx_data = web_utils.encode_add_liquidity_batch([order])

        # Execute transaction
        wallet = self._canonic_auth.address
        gas_price = await self._rpc_client.get_gas_price()
        nonce = await self._rpc_client.get_transaction_count(wallet)

        tx = self._canonic_auth.build_transaction(
            to=CONSTANTS.CLOB_CONTRACT,
            data=tx_data,
            gas=CONSTANTS.PLACE_ORDER_GAS_LIMIT,
            gas_price=gas_price,
            nonce=nonce,
        )

        signed_tx = self._canonic_auth.sign_transaction(tx)
        tx_hash = await self._rpc_client.send_raw_transaction("0x" + signed_tx.hex())
        self.logger().info(f"Order placement tx: {tx_hash}")

        # Wait for receipt
        receipt = await self._rpc_client.wait_for_transaction_receipt(tx_hash)
        if int(receipt.get("status", "0x0"), 16) != 1:
            raise Exception(f"Order placement failed: {receipt}")

        # Parse order ID from receipt logs
        exchange_order_id = self._parse_order_id_from_receipt(receipt)

        # Track the order
        if exchange_order_id:
            self._active_orders[order_id] = ActiveOrder(
                order_id=exchange_order_id,
                client_order_id=order_id,
                rung=rung,
                is_ask=is_ask,
                amount=amount_wei,
                placed_at=datetime.now(timezone.utc),
            )

        return str(exchange_order_id or tx_hash), time.time()

    def _parse_order_id_from_receipt(self, receipt: Dict) -> Optional[int]:
        """Parse order ID from transaction receipt logs."""
        try:
            logs = receipt.get("logs", [])

            for log in logs:
                # Check if this is from CLOB contract
                log_address = log.get("address", "")
                if log_address.lower() != CONSTANTS.CLOB_CONTRACT.lower():
                    continue

                topics = log.get("topics", [])
                if not topics:
                    continue

                # Check for OrderPlaced event
                topic0 = topics[0]
                if isinstance(topic0, str) and topic0.lower().startswith(CONSTANTS.ORDER_PLACED_TOPIC[:10].lower()):
                    # Order ID is in data field (first 32 bytes)
                    data = log.get("data", "0x")
                    if isinstance(data, str):
                        data = data[2:] if data.startswith("0x") else data
                        if len(data) >= 64:
                            order_id = int(data[:64], 16)
                            return order_id

            return None

        except Exception as e:
            self.logger().error(f"Error parsing order ID from receipt: {e}")
            return None

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """Cancel an order on MAOB."""
        active_order = self._active_orders.get(order_id)
        if not active_order:
            # Try to get exchange order ID from tracked order
            exchange_order_id = tracked_order.exchange_order_id
            if not exchange_order_id:
                raise Exception(f"No exchange order ID for {order_id}")
            maob_order_id = int(exchange_order_id)
        else:
            maob_order_id = active_order.order_id

        # Encode cancelOrder call
        tx_data = web_utils.encode_cancel_order(maob_order_id, withdraw_after=True)

        # Execute transaction
        wallet = self._canonic_auth.address
        gas_price = await self._rpc_client.get_gas_price()
        nonce = await self._rpc_client.get_transaction_count(wallet)

        tx = self._canonic_auth.build_transaction(
            to=CONSTANTS.CLOB_CONTRACT,
            data=tx_data,
            gas=CONSTANTS.CANCEL_GAS_LIMIT,
            gas_price=gas_price,
            nonce=nonce,
        )

        signed_tx = self._canonic_auth.sign_transaction(tx)
        tx_hash = await self._rpc_client.send_raw_transaction("0x" + signed_tx.hex())
        self.logger().info(f"Cancel order tx: {tx_hash}")

        # Wait for receipt
        receipt = await self._rpc_client.wait_for_transaction_receipt(tx_hash)
        if int(receipt.get("status", "0x0"), 16) != 1:
            raise Exception(f"Cancel order failed: {receipt}")

        # Remove from tracking
        if order_id in self._active_orders:
            del self._active_orders[order_id]

        return True

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """Cancel all active orders."""
        results = []

        for order_id, active_order in list(self._active_orders.items()):
            try:
                tracked_order = self._order_tracker.all_updatable_orders.get(order_id)
                if tracked_order:
                    await self._place_cancel(order_id, tracked_order)
                    results.append(CancellationResult(order_id=order_id, success=True))
                else:
                    # Direct cancellation without tracked order
                    tx_data = web_utils.encode_cancel_order(active_order.order_id, withdraw_after=True)
                    wallet = self._canonic_auth.address
                    gas_price = await self._rpc_client.get_gas_price()
                    nonce = await self._rpc_client.get_transaction_count(wallet)

                    tx = self._canonic_auth.build_transaction(
                        to=CONSTANTS.CLOB_CONTRACT,
                        data=tx_data,
                        gas=CONSTANTS.CANCEL_GAS_LIMIT,
                        gas_price=gas_price,
                        nonce=nonce,
                    )

                    signed_tx = self._canonic_auth.sign_transaction(tx)
                    await self._rpc_client.send_raw_transaction("0x" + signed_tx.hex())
                    results.append(CancellationResult(order_id=order_id, success=True))

            except Exception as e:
                self.logger().error(f"Error cancelling order {order_id}: {e}")
                results.append(CancellationResult(order_id=order_id, success=False))

        self._active_orders.clear()
        return results

    async def _get_mid_price(self) -> Optional[float]:
        """Fetch current mid price from MAOB contract."""
        try:
            call_data = web_utils.encode_get_mid_price()
            result = await self._rpc_client.eth_call(CONSTANTS.CLOB_CONTRACT, call_data)

            if result and result != "0x":
                price, precision, updated_at = web_utils.decode_get_mid_price(result)
                self._last_mid_price = price / precision
                return self._last_mid_price
        except Exception as e:
            self.logger().warning(f"Error fetching mid price: {e}\n{traceback.format_exc()}")
            return None

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """Request order status via previewOrder."""
        try:
            active_order = self._active_orders.get(tracked_order.client_order_id)
            if not active_order:
                exchange_order_id = tracked_order.exchange_order_id
                if not exchange_order_id:
                    return OrderUpdate(
                        client_order_id=tracked_order.client_order_id,
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=time.time(),
                        new_state=OrderState.FAILED,
                    )
                maob_order_id = int(exchange_order_id)
            else:
                maob_order_id = active_order.order_id

            # Query order status
            call_data = web_utils.encode_preview_order(maob_order_id)
            result = await self._rpc_client.eth_call(CONSTANTS.CLOB_CONTRACT, call_data)

            if not result or result == "0x":
                return OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=time.time(),
                    new_state=OrderState.FAILED,
                )

            unfilled_input, claimable_output = web_utils.decode_preview_order(result)

            # Determine order state
            if unfilled_input == 0 and claimable_output == 0:
                # Fully filled and claimed, or cancelled
                new_state = OrderState.FILLED
            elif unfilled_input == 0 and claimable_output > 0:
                # Fully filled, pending claim
                new_state = OrderState.FILLED
            elif claimable_output > 0:
                # Partially filled
                new_state = OrderState.PARTIALLY_FILLED
            else:
                # Open
                new_state = OrderState.OPEN

            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(maob_order_id),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=new_state,
            )

        except Exception as e:
            self.logger().error(f"Error requesting order status: {e}")
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=tracked_order.current_state,
            )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """Get all trade updates for an order."""
        trade_updates = []

        try:
            active_order = self._active_orders.get(order.client_order_id)
            if not active_order:
                return trade_updates

            # Query order status
            call_data = web_utils.encode_preview_order(active_order.order_id)
            result = await self._rpc_client.eth_call(CONSTANTS.CLOB_CONTRACT, call_data)

            if not result or result == "0x":
                return trade_updates

            unfilled_input, claimable_output = web_utils.decode_preview_order(result)

            if claimable_output > 0:
                # Calculate fill
                original_amount = active_order.amount
                filled_amount_wei = original_amount - unfilled_input

                if filled_amount_wei > 0:
                    # Determine fill amounts based on order side
                    if active_order.is_ask:
                        # Sold WETH, received USDm
                        fill_base_amount = canonic_utils.wei_to_token(filled_amount_wei, CONSTANTS.BASE_DECIMALS)
                        fill_quote_amount = canonic_utils.wei_to_token(claimable_output, CONSTANTS.QUOTE_DECIMALS)
                        fill_price = fill_quote_amount / fill_base_amount if fill_base_amount > 0 else Decimal(0)
                    else:
                        # Spent USDm, received WETH
                        fill_quote_amount = canonic_utils.wei_to_token(filled_amount_wei, CONSTANTS.QUOTE_DECIMALS)
                        fill_base_amount = canonic_utils.wei_to_token(claimable_output, CONSTANTS.BASE_DECIMALS)
                        fill_price = fill_quote_amount / fill_base_amount if fill_base_amount > 0 else Decimal(0)

                    # Calculate fee (taker fee)
                    fee_amount = fill_quote_amount * Decimal(str(CONSTANTS.TAKER_FEE_PERCENT))

                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=order.quote_asset,
                        flat_fees=[TokenAmount(amount=fee_amount, token=order.quote_asset)],
                    )

                    trade_update = TradeUpdate(
                        trade_id=f"{active_order.order_id}_{int(time.time())}",
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(active_order.order_id),
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_base_amount,
                        fill_quote_amount=fill_quote_amount,
                        fill_price=fill_price,
                        fill_timestamp=time.time(),
                    )
                    trade_updates.append(trade_update)

        except Exception as e:
            self.logger().error(f"Error getting trade updates: {e}")

        return trade_updates

    async def _update_balances(self):
        """Update account balances."""
        wallet = self._canonic_auth.address
        if not wallet:
            return

        try:
            # Query WETH and USDm balances
            calls = [
                {"to": CONSTANTS.WETH_CONTRACT, "data": web_utils.encode_balance_of(wallet)},
                {"to": CONSTANTS.USDM_CONTRACT, "data": web_utils.encode_balance_of(wallet)},
            ]

            results = await self._rpc_client.batch_call(calls)

            if results and len(results) >= 2:
                weth_balance = web_utils.decode_balance_of(results[0].get("result", "0x0"))
                usdm_balance = web_utils.decode_balance_of(results[1].get("result", "0x0"))

                self._account_balances["WETH"] = Decimal(weth_balance) / Decimal(10 ** CONSTANTS.BASE_DECIMALS)
                self._account_balances["USDm"] = Decimal(usdm_balance) / Decimal(10 ** CONSTANTS.QUOTE_DECIMALS)

                # For available balances, subtract amounts in active orders
                weth_in_orders = sum(
                    Decimal(o.amount) / Decimal(10 ** CONSTANTS.BASE_DECIMALS)
                    for o in self._active_orders.values()
                    if o.is_ask
                )
                usdm_in_orders = sum(
                    Decimal(o.amount) / Decimal(10 ** CONSTANTS.QUOTE_DECIMALS)
                    for o in self._active_orders.values()
                    if not o.is_ask
                )

                self._account_available_balances["WETH"] = self._account_balances["WETH"] - weth_in_orders
                self._account_available_balances["USDm"] = self._account_balances["USDm"] - usdm_in_orders

        except Exception as e:
            self.logger().error(f"Error updating balances: {e}")

    async def _update_trading_fees(self):
        """Update trading fees - hardcoded for MAOB."""
        pass

    async def _user_stream_event_listener(self):
        """Listen for user stream events."""
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")

                if event_type == "order_update":
                    await self._process_order_update(event_message)
                elif event_type == "order_placed":
                    await self._process_order_placed(event_message)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error processing user stream event: {e}", exc_info=True)

    async def _process_order_update(self, event: Dict):
        """Process an order status update event."""
        order_id = event.get("order_id")

        # Find matching tracked order
        for client_order_id, active_order in self._active_orders.items():
            if active_order.order_id == order_id:
                tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                if tracked_order:
                    if event.get("fully_filled"):
                        order_update = OrderUpdate(
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_id),
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=time.time(),
                            new_state=OrderState.FILLED,
                        )
                        self._order_tracker.process_order_update(order_update)
                    elif event.get("has_fill"):
                        order_update = OrderUpdate(
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_id),
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=time.time(),
                            new_state=OrderState.PARTIALLY_FILLED,
                        )
                        self._order_tracker.process_order_update(order_update)
                break

    async def _process_order_placed(self, event: Dict):
        """Process an order placed confirmation event."""
        # This is handled in _place_order, but can be used for reconciliation
        pass

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Get last traded price (mid price for MAOB)."""
        mid_price = await self._get_mid_price()
        return mid_price if mid_price else 0.0

    async def _make_network_check_request(self):
        """Check network connectivity."""
        await self._rpc_client.get_block_number()

    async def _make_trading_rules_request(self) -> Any:
        """Get trading rules - not applicable for on-chain DEX."""
        return []

    async def _make_trading_pairs_request(self) -> Any:
        """Get trading pairs - hardcoded for MAOB."""
        return []
