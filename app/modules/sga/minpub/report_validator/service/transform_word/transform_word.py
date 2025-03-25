import pandas as pd
from docx import Document

def extract_data(file_paths):
    df1 = pd.read_excel(file_paths["file1"])
    df2 = pd.read_excel(file_paths["file2"])

    doc = Document(file_paths["word_file"])
    table = doc.tables[0]
    data = [[cell.text for cell in row.cells] for row in table.rows]
    df_word = pd.DataFrame(data[1:], columns=data[0])

    return { "df1": df1, "df2": df2, "df_word": df_word}

                                                                                                                                                                                                                                                                                                                        