import pandas as pd

df = pd.read_csv('data/master.csv')

print('--- CATEGORIAS EXACTAS EN MASTER ---')
for c in sorted(df['categoria'].unique()):
    print(f'  "{c}"')

print()
productos_check = [
    'EXPECTOVIC', 'EXPULSATOX', 'SILUET 40',
    'NEXT DEFEN-C', 'BIO ENERGY', 'BIO-ELECTRO',
    'CHAO FEBREDOL G3A', 'GOICOECHEA DIABET'
]
print('--- CATEGORIA ACTUAL DE PRODUCTOS A OVERRIDEAR ---')
for p in productos_check:
    rows = df[df['producto'] == p][['producto','categoria','sub_categoria']].drop_duplicates()
    if not rows.empty:
        for _, r in rows.iterrows():
            print(f'  {r.producto} | {r.categoria} | {r.sub_categoria}')
    else:
        print(f'  {p} | NO ENCONTRADO')