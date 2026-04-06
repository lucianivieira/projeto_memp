# import_simples.py
import duckdb
from pathlib import Path
import sys

CSV_DIR = Path(r"C:\Users\francisco.vieira\OneDrive - EBSERH\Dropbox\#Jobs\Projeto MEMP\rf_cnpj_csv\2026-01")
DB_PATH = Path(r"C:\Users\francisco.vieira\OneDrive - EBSERH\Dropbox\#Jobs\Projeto MEMP\cnpj_2026_01.duckdb")

def main():
    simples_files = sorted(list(CSV_DIR.glob("*SIMPLES*")))
    if not simples_files:
        raise RuntimeError(f"Nenhum arquivo *SIMPLES* encontrado em {CSV_DIR}")

    simples_file = simples_files[0]
    print("SIMPLES:", simples_file)

    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=8;")

    con.execute("DROP TABLE IF EXISTS simples;")
    con.execute(f"""
        CREATE TABLE simples AS
        SELECT * FROM read_csv_auto(
            '{str(simples_file).replace("'", "''")}',
            sep=';',
            header=true,
            encoding='latin-1',
            union_by_name=true,
            strict_mode=false,
            all_varchar=true,
            ignore_errors=true
        );
    """)

    count_simples = con.execute("SELECT COUNT(*) FROM simples").fetchone()[0]
    print("COUNT simples:", count_simples)

    con.close()
    print("✅ OK - simples importado")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ ERRO:", e)
        sys.exit(1)
