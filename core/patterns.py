from typing import Dict, Any, Tuple

class PatternMatcher:
    def __init__(self, patterns: list[Dict[str, Any]]):
        self.patterns = patterns

    def match(self, payload: Any) -> Tuple[str, Dict[str, Any]] | tuple[None, None]:
        # Prefer dict keys matches; if multiple match, choose the most specific (largest required key set)
        if isinstance(payload, dict):
            keys = set(payload.keys())
            best: Tuple[int, Dict[str, Any]] | None = None
            for p in self.patterns:
                m = p.get('match', {})
                req = set(m.get('keys', []))
                if req and req.issubset(keys):
                    # Score based on exactness of match - prefer exact matches
                    exactness_score = 1000 if len(req) == len(keys) else len(req)
                    cand = (exactness_score, p)
                    if best is None or cand[0] > best[0]:
                        best = cand
            if best is not None:
                chosen = best[1]
                return chosen['name'], chosen
        
        # Check nested 'd' structure for array enveloped pattern
        if isinstance(payload, dict) and 'd' in payload and isinstance(payload['d'], dict):
            d_keys = set(payload['d'].keys())
            best: Tuple[int, Dict[str, Any]] | None = None
            for p in self.patterns:
                m = p.get('match', {})
                req = set(m.get('keys', []))
                if req and req.issubset(d_keys):
                    # Score based on exactness of match for nested structure
                    exactness_score = 1000 if len(req) == len(d_keys) else len(req)
                    cand = (exactness_score, p)
                    if best is None or cand[0] > best[0]:
                        best = cand
            if best is not None:
                chosen = best[1]
                return chosen['name'], chosen
        
        # schema based (very light check) - fallback for array enveloped
        for p in self.patterns:
            m = p.get('match', {})
            schema = m.get('schema')
            if not schema:
                continue
            if isinstance(payload, dict) and 'd' in payload and 'ts' in payload:
                return p['name'], p
        return None, None

    @staticmethod
    def derive_columns_auto(topic: str, payload: Any) -> Dict[str, Any]:
        cols: Dict[str, Any] = {'topic': 'string'}
        # For dict payloads: top-level keys become columns, values decide types
        if isinstance(payload, dict):
            # Special: flatten 'd' object of lists and include 'ts' if present
            if 'd' in payload and isinstance(payload['d'], dict):
                d = payload['d']
                for k, v in d.items():
                    if k in cols:
                        continue
                    # infer type from first element if list
                    vv = v[0] if isinstance(v, list) and v else v
                    if isinstance(vv, int):
                        cols[k] = 'int'
                    elif isinstance(vv, float):
                        cols[k] = 'float'
                    elif isinstance(vv, str):
                        cols[k] = 'string'
                    else:
                        cols[k] = 'json'
                if 'ts' in payload:
                    cols['ts'] = 'string'
            else:
                for k, v in payload.items():
                    if k in cols:
                        continue
                    if isinstance(v, int):
                        cols[k] = 'int'
                    elif isinstance(v, float):
                        cols[k] = 'float'
                    elif isinstance(v, str):
                        cols[k] = 'string'
                    else:
                        cols[k] = 'json'
        else:
            cols['payload'] = 'json'
        return cols

    @staticmethod
    def to_row_auto(topic: str, payload: Any) -> Dict[str, Any]:
        row: Dict[str, Any] = {'topic': topic}
        if isinstance(payload, dict):
            if 'd' in payload and isinstance(payload['d'], dict):
                d = payload['d']
                for k, v in d.items():
                    row[k] = v[0] if isinstance(v, list) and v else v
                if 'ts' in payload:
                    row['ts'] = payload['ts']
            else:
                row.update(payload)
        else:
            row['payload'] = payload
        return row
