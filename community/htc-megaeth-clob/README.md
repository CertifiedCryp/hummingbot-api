# HTC - Megaeth CLOB Connector

Community Hummingbot drop-in for the MegaETH CLOB (Canonic MAOB, chain `4326`).

**Not affiliated with Hummingbot / CoinAlpha.**  
**Not a PR against `gateway` or `hummingbot` main.** Stay on this fork so they do not own maintenance.

This repo is the **API orchestrator**. These files do not run inside hummingbot-api. Copy them into a Hummingbot **client**:

```text
hummingbot/connector/exchange/canonic/
```

Client fork if you need one: [CertifiedCryp/hummingbot](https://github.com/CertifiedCryp/hummingbot).

## Rules

- Pure EOA. No session keys.
- Phase 1: mid / depth / balances first. No live size until those return.
- No keys, wallets, or live sizes in this folder.
- Clip and stop live in the HTC kernel, not here.

## Contracts (MegaETH)

| | |
|---|---|
| WETH | `0x4200000000000000000000000000000000000006` |
| USDm | `0xFAfDdbb3FC7688494971a79cc65DCa3EF82079E7` |
| MAOB | `0x23469683e25b780DFDC11410a8e83c923caDF125` |
| Pair | `WETH-USDm` |
| Fees | maker 0 / taker 0.03% |

## Install

See [INSTALL.md](INSTALL.md).

## Honest limits (MVP)

1. Rung map is linear (`bps_to_rung`). Production should call `getRungs()`.
2. Order book is a thin synthetic around mid until `getDepth` ABI is locked.
3. User stream is a heartbeat poll. WS `eth_subscribe` not wired.
4. `ORDER_PLACED_TOPIC` is a placeholder.

## Discord one-liner

HTC MegaETH CLOB connector. Community fork. Not for merge into gateway. Copy into `hummingbot/connector/exchange/canonic/`.
