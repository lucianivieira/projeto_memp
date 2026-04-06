import duckdb
from pathlib import Path

csv_dir = Path(r"C:\Users\francisco.vieira\OneDrive - EBSERH\Dropbox\#Jobs\Projeto MEMP\rf_cnpj_csv\2026-01")
db_path = Path(r"C:\Users\francisco.vieira\OneDrive - EBSERH\Dropbox\#Jobs\Projeto MEMP\cnpj_2026_01.duckdb")

def find_files(patterns):
    files = []
    for pat in patterns:
        files += list(csv_dir.glob(pat))
    # remove duplicados
    files = sorted(set(files))
    return [str(f) for f in files]

# ✅ padrões adaptados ao seu print
emp_files = find_files(["*EMPRECSV*", "*emprescsv*"])
est_files = find_files(["*ESTABELE*", "*estable*"])
sim_files = find_files(["*SIMPLES*", "*simples*"])

print("EMPRESAS:", len(emp_files))
if emp_files: print("  ex:", emp_files[0])
print("ESTABELECIMENTOS:", len(est_files))
if est_files: print("  ex:", est_files[0])
print("SIMPLES:", len(sim_files))
if sim_files: print("  ex:", sim_files[0])

if not emp_files:
    raise RuntimeError("Não encontrei arquivos de EMPRESAS. Confirme se há 'EMPRECSV' no nome e se está na pasta correta.")
if not est_files:
    raise RuntimeError("Não encontrei arquivos de ESTABELECIMENTOS. Confirme se há 'ESTABE' no nome e se está na pasta correta.")
if not sim_files:
    raise RuntimeError("Não encontrei arquivo SIMPLES/MEI. Confirme se há 'SIMPLES' no nome e se está na pasta correta.")

con = duckdb.connect(str(db_path))
con.execute("PRAGMA threads=8;")

# Dica: se der algum problema de leitura, habilite ignore_errors=true (não recomendo de primeira)
def create_from_files(table, files):
    files_sql = "[" + ",".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
    con.execute(f"DROP TABLE IF EXISTS {table};")

    attempts = [
        ("AUTO", ""),  # sem encoding
        ("UTF8", "encoding='utf-8',"),
        ("LATIN1", "encoding='latin-1',"),
    ]

    last_err = None
    for label, enc_sql in attempts:
        try:
            print(f"-> Criando {table} com {label}")
            con.execute(f"""
                CREATE TABLE {table} AS
                SELECT * FROM read_csv_auto(
                    {files_sql},
                    sep=';',
                    header=true,
                    {enc_sql}
                    union_by_name=true
                );
            """)
            print(f"✅ {table} OK ({label})")
            return
        except Exception as e:
            print(f"   falhou ({label}): {e}")
            last_err = e

    raise last_err


create_from_files("empresas", emp_files)
create_from_files("estabelecimentos", est_files)

# Simples normalmente é 1 arquivo, mas vamos permitir lista também
create_from_files("simples", sim_files)

con.close()
print("✅ DuckDB criado e tabelas importadas em:", db_path)
