def find_contract_blocks(code: str) -> list[str]:
    blocks, i, n = [], 0, len(code)
    while i < n:
        start = code.find("\ncontract ", i)
        if start == -1: break
        open_brace = code.find("{", start)
        if open_brace == -1: break
        depth, j = 1, open_brace + 1
        while j < n and depth > 0:
            ch = code[j]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            j += 1
        if depth == 0:
            blocks.append(code[start:j])
            i = j
        else:
            break
    return blocks


def find_transfer_functions(contract_blocks: list[str]):
    out = []
    for block in contract_blocks:
        lines = block.splitlines()
        functions = []
        fname = ""
        body = ""
        in_fn = False
        is_mint = is_burn = False
        cname = block.split("contract ", 1)[1].split("{", 1)[0].strip().split()[0]

        for ln in lines:
            if "function " in ln and " transfer(" not in ln:
                if in_fn:
                    if ".transfer(" in body and "super.transfer(" not in body and "owner.transfer(" not in body:
                        functions.append({"name": fname, "body": body.strip()})
                # новая
                in_fn = True
                fname = ln.split("function ", 1)[1].split("(", 1)[0].strip()
                body = ln
                if " burn" in ln: is_burn = True
                if " mint" in ln: is_mint = True
            elif in_fn:
                body += "\n" + ln
                if "}" in ln:
                    if ".transfer(" in body and "super.transfer(" not in body and "owner.transfer(" not in body:
                        functions.append({"name": fname, "body": body.strip()})
                    in_fn = False
                    fname = ""
                    body = ""

        if functions or (is_burn and is_mint):
            out.append({"contract": cname, "functions": functions, "burnMint": (is_burn and is_mint)})
    return out


def analyze_extract(address: str, code: str) -> dict:
    if not code:
        return {"address": address, "contracts": [], "functions": [], "burnMint": False}
    blocks = find_contract_blocks(code)
    tf = find_transfer_functions(blocks)
    ret = {"address": address, "contracts": [], "functions": [], "burnMint": False}
    for x in tf:
        ret["contracts"].append(x["contract"])
        if x.get("burnMint"): ret["burnMint"] = True
        for f in x.get("functions", []):
            ret["functions"].append(f["name"])
    return ret
