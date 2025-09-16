-- SFCS column deduplication function (callable in SQL)
CREATE OR REPLACE FUNCTION dedupe_columns(cols ARRAY)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'dedupe_columns'
AS
$$
def dedupe_columns(cols):
    """SFCS column deduplication function"""
    if not cols:
        return []
        
    seen = {}
    result = []
    for col in cols:
        base = col
        count = seen.get(base, 0)
        if count == 0:
            result.append(base)
        else:
            result.append(f"{base}_{count}")
        seen[base] = count + 1
    return result
$$;