import duckdb
from pathlib import Path
import sys

CSV_DIR = Path(r"C:\Users\francisco.vieira\OneDrive - EBSERH\Dropbox\#Jobs\Projeto MEMP\rf_cnpj_csv\2026-01")
DB_PATH = Path(r"C:\Users\francisco.vieira\OneDrive - EBSERH\Dropbox\#Jobs\Projeto MEMP\cnpj_2026_01.duckdb")

def sql_list(files):
    return "[" + ",".join("'" + f.replace("'", "''") + "'" for f in files) + "]"

def main():
    emp_files = sorted([str(p) for p in CSV_DIR.glob("*EMPRECSV*")])
    if not emp_files:
        raise RuntimeError("Não encontrei arquivos *EMPRECSV*")

    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=8;")

    files_sql = sql_list(emp_files)
    con.execute("DROP TABLE IF EXISTS empresas;")
    con.execute(f"""
        CREATE TABLE empresas AS
        SELECT * FROM read_csv_auto(
            {files_sql},
            sep=';',
            header=false,
            encoding='latin-1',
            union_by_name=true,
            strict_mode=false,
            all_varchar=true,
            ignore_errors=true
        );
    """)

    print("COUNT empresas:", con.execute("SELECT COUNT(*) FROM empresas").fetchone()[0])
    con.close()
    print("✅ OK empresas (header=false)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ ERRO:", e)
        sys.exit(1)
