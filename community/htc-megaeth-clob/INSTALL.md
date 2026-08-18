# HTC - Megaeth CLOB Connector — Install & Smoke Test

Drop-in package for Hummingbot on **MegaETH** (chain 4326).
Community fork. Not for merge into gateway.

## Contracts (locked)

| Asset | Address |
|-------|---------|
| WETH | `0x4200000000000000000000000000000000000006` |
| USDm | `0xFAfDdbb3FC7688494971a79cc65DCa3EF82079E7` |
| MAOB WETH-USDm | `0x23469683e25b780DFDC11410a8e83c923caDF125` |

## Install into your Hummingbot tree

```bash
HBOT=~/repos_3-4-26/hummingbot   # adjust path
DEST="$HBOT/hummingbot/connector/exchange/canonic"

mkdir -p "$DEST"
cp -v canonic_*.py __init__.py "$DEST/"
ls -la "$DEST"
```

## Register connector (one-time)

If `canonic` is not already in `hummingbot/client/settings.py` / connector settings, add:

```python
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from pydantic import Field, SecretStr

class CanonicConfigMap(BaseConnectorConfigMap):
    connector: str = "canonic"
    canonic_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MegaETH private key (0x...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

KEYS = CanonicConfigMap.model_construct()
```

And a `ConnectorSetting` entry with `name="canonic"`, `type=ConnectorType.Exchange`,
`example_pair="WETH-USDm"`, `centralised=False`, `use_ethereum_wallet=True`,
`trade_fee_schema` maker 0 / taker 0.03%.

## Smoke tests (no capital at risk)

```bash
cd "$HBOT"

# 1) Discovery
python -c "
from hummingbot.client.settings import AllConnectorSettings
print('canonic' in AllConnectorSettings.get_connector_settings())
"

# 2) RPC / chain
python -c "
from hummingbot.connector.exchange.canonic.canonic_web_utils import MegaETHRPCClient
from hummingbot.connector.exchange.canonic.canonic_constants import CHAIN_ID, CLOB_CONTRACT
import asyncio
async def main():
    c = MegaETHRPCClient()
    n = await c.get_block_number()
    print(f'block={n} expected_chain={CHAIN_ID} clob={CLOB_CONTRACT}')
    await c.close()
asyncio.run(main())
"

# 3) Mid price
python -c "
from hummingbot.connector.exchange.canonic.canonic_web_utils import MegaETHRPCClient, encode_get_mid_price, decode_get_mid_price
from hummingbot.connector.exchange.canonic.canonic_constants import CLOB_CONTRACT
import asyncio
async def main():
    c = MegaETHRPCClient()
    r = await c.eth_call(CLOB_CONTRACT, encode_get_mid_price())
    price, prec, ts = decode_get_mid_price(r)
    print(f'raw={price} prec={prec} mid≈{price/prec if prec else price}')
    await c.close()
asyncio.run(main())
"
```

## Connect in CLI

```text
bin/hummingbot
>>> connect canonic
# paste private key when prompted
>>> balance
```

## Known MVP limits

1. **Rung mapping** — `bps_to_rung` is linear; production should call `getRungs()`.
2. **Order book** — synthetic thin book around mid until `getDepth` ABI is fully decoded.
3. **User stream** — poll/heartbeat only; WS `eth_subscribe logs` not wired yet.
4. **Event topic** — `ORDER_PLACED_TOPIC` is placeholder; tighten after one live placement.

## Safe first live

- Approve + one tiny limit (e.g. 0.001 WETH) far from mid.
- Confirm on https://canonic.trade
- Cancel via connector or UI.
- Only then enable PMM / XEMM.

## Package files

```
htc-megaeth-clob/
  README.md
  INSTALL.md
  __init__.py
  canonic_auth.py
  canonic_constants.py
  canonic_utils.py
  canonic_web_utils.py
  canonic_api_order_book_data_source.py
  canonic_api_user_stream_data_source.py
  canonic_exchange.py
```
