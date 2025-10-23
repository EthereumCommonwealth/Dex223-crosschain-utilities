# utils/export.py
from decimal import Decimal
from services.scanner import TokenScanResult, BalanceRecord  # adjust the path to suit your project
from utils.iohelpers import number_with_commas


def result_to_public_json(
        res: TokenScanResult,
        exclude_by_list: bool,
        exclude_by_mint: bool,
        exclude_by_extract: bool,
) -> dict:
    """
    Converts a single TokenScanResult into a closed JSON format.

    - price/asDollar/amount -> numeric values ​​(float)
    - records -> [{amount (raw as string), roundedAmount (float), DollarValue (string with commas), contract}]
    - exclusion (by list/mint/extract) applied to conditions (amount/in US dollars),
    but we don't remove anything from the records (we can also filter if necessary).
    """

    def is_excluded(rec: BalanceRecord) -> bool:
        return ((exclude_by_list and rec.exclude) or
                (exclude_by_extract and res.hasExtract) or
                (exclude_by_mint and res.hasBurnMint))

    # Amount only for NOT excluded items
    sum_amount = Decimal(0)
    for r in res.records:
        if not is_excluded(r):
            sum_amount += r.amount

    out_records = []
    for r in sorted(res.records, key=lambda x: x.amount, reverse=True):
        raw_str = str(r.raw_balance)
        rounded = float(r.amount)
        usd_val = float(r.amount * res.price)
        out_records.append({
            "amount": raw_str,
            "roundedAmount": rounded,
            "dollarValue": number_with_commas(usd_val, 2),  # "14,647,715.61"
            "contract": r.contract,
        })

    price_num = float(res.price)
    as_dollar_num = float(sum_amount * res.price)

    return {
        "tokenAddress": res.tokenAddress,
        "ticker": res.ticker,
        "decimals": res.decimals,
        "price": price_num,
        "logo": res.logo,
        "hasExtract": res.hasExtract,
        "hasBurnMint": res.hasBurnMint,
        "records": out_records,
        "asDollar": as_dollar_num,
        "amount": float(sum_amount),
    }


def results_to_public_json(
        results: list[TokenScanResult],
        exclude_by_list: bool,
        exclude_by_mint: bool,
        exclude_by_extract: bool,
) -> list[dict]:
    return [
        result_to_public_json(r, exclude_by_list, exclude_by_mint, exclude_by_extract)
        for r in results
    ]
