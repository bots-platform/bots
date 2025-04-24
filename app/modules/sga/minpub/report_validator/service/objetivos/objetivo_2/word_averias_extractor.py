# word_extractor.py

import pandas as pd
from docx import Document
from typing import List, Dict
from utils.logger_config import get_sga_logger

logger = get_sga_logger()

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper

@log_exceptions
def extract_averias_table(path_docx: str, table_index: int = 0) -> pd.DataFrame:
    """
    Load the specified table from a .docx and return it as a DataFrame.
    We assume the first row is the header.
    """
    doc = Document(path_docx)
    if table_index >= len(doc.tables):
        raise IndexError(f"Document only has {len(doc.tables)} tables.")

    table = doc.tables[table_index]

    headers = [cell.text.strip() for cell in table.rows[0].cells]

    records: List[Dict] = []
    for row in table.rows[1:]:
        vals = [cell.text.strip() for cell in row.cells]
     
        if len(vals) < len(headers):
            vals += [""] * (len(headers) - len(vals))
        records.append(dict(zip(headers, vals)))

    df = pd.DataFrame.from_records(records)
    return df

