# bmbdump

`bmbdump` is a small set of Python utilities to produce and publish **JSONL block/transaction dumps** for **BTCMobick**, a **Bitcoin hard fork**.

BTCMobick’s public chain data (including historical transactions) is expected to be **deleted / pruned**, so this repository exists to **preserve** the full on-chain history for a specific block range and to make it **shareable and reproducible** via GitHub Releases.

> **Large dataset files are distributed via GitHub Releases**, not tracked in the Git repository.

---

## What’s Included

This repo contains scripts to:
- download blocks for a given height range,
- fetch all transactions contained in those blocks,
- export results to **JSON Lines**:
  - `blocks.jsonl` (one record per block)
  - `txs.jsonl` (one record per transaction)

---

## Repository Structure

- `download_blocks.py` — download raw block data for a given range
- `get_tx.py` — fetch transaction details
- `blocks_to_jsonl.py` — convert downloaded block data to `blocks.jsonl`
- `txs_to_jsonl.py` — convert downloaded tx data to `txs.jsonl`
- `logs/` — logs / intermediate artifacts (if used)

---

## Dataset Releases

### Block dump: 556,760–855,698 (inclusive)

The dataset is published as a single **.7z** archive in the GitHub Releases page.

**Archive**
- `bmbdump_556760_855698.7z`

**Contains**
- `blocks.jsonl` — one JSON object per line (one block per line)
- `txs.jsonl` — one JSON object per line (one transaction per line)

**SHA-256**
- `bmbdump_556760_855698.7z`: `EF01FBE4447936B722D281C1E9984A34A41399F02CD9ED35DD25C7B57C51E0AA`

---

## Extract & Verify

### Extract
```bash
7z x bmbdump_556760_855698.7z
````

### Verify (Linux/macOS)

```bash
echo "EF01FBE4447936B722D281C1E9984A34A41399F02CD9ED35DD25C7B57C51E0AA  bmbdump_556760_855698.7z" | sha256sum -c -
```

### Verify (Windows PowerShell)

```powershell
(Get-FileHash .\bmbdump_556760_855698.7z -Algorithm SHA256).Hash
```

---

## JSONL Format

* Encoding: UTF-8
* `blocks.jsonl`: one JSON object per line (one block per line)
* `txs.jsonl`: one JSON object per line (one transaction per line)

---

## Purpose / Motivation

BTCMobick is a Bitcoin hard fork, and the public chain data (including transactions) is expected to be removed.
This dump is intended to preserve:

* the complete block information for the specified height range, and
* the full list of transactions contained within those blocks,
  in a simple, stream-friendly format suitable for later indexing and analysis.

---

## Issues

If you discover missing or inconsistent data, please open an issue and include:

* block height(s)
* transaction identifier(s) (if applicable)
* any logs or steps to reproduce

```
::contentReference[oaicite:0]{index=0}
```
