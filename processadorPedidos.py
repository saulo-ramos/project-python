import pandas as pd
from thefuzz import fuzz
from datetime import datetime
import os

class ProcesadorPedidos:
    def __init__(self, ruta_reporte, ruta_padrao, ruta_clientes, ruta_salida):
        self.ruta_reporte = ruta_reporte
        self.ruta_padrao = ruta_padrao
        self.ruta_clientes = ruta_clientes
        self.ruta_salida = ruta_salida
        self.df = None
        self.df_padrao = None
        self.df_clientes = None
        self.dicionario_padrao = {}

    def cargar_datos(self):
        try:
            self.df = pd.read_excel(self.ruta_reporte, sheet_name='Relatório')
            self.df_padrao = pd.read_excel(self.ruta_padrao, sheet_name='Planilha1')
            self._preparar_dicionario_padrao()
            self.df_clientes = pd.read_excel(self.ruta_clientes)
            print("✅ Archivos cargados exitosamente.")
        except Exception as e:
            print(f"❌ Error al cargar archivos: {e}")

    def _preparar_dicionario_padrao(self):
        for _, row in self.df_padrao.iterrows():
            chave = str(row.iloc[0]).lower().strip()
            self.dicionario_padrao[chave] = tuple(row.iloc[1:])

    def limpiar_documento(self):
        if self.df is None: 
            print("❌ DataFrame não carregado!")
            return

        # 1. Preparación de columnas
        self.df.insert(0, 'descricao', '')
        self._propagar_descripciones()

        # 3. IMPORTANTE: Ordenar DESPUÉS de propagar
        nome_coluna_b = self.df.columns[1]
        self.df = self.df.sort_values(by=nome_coluna_b, ascending=True, ignore_index=True)
        
        # 4. Eliminar filas post 'Data Emissão'
        idx_filtro = self.df[self.df[nome_coluna_b].astype(str).str.lower().str.contains('data emissão')].index
        if not idx_filtro.empty:
            self.df = self.df.iloc[:idx_filtro[0]]

        # 5. Adicionar colunas categoria e calibre
        self.df['categoria'] = None
        self.df['calibre'] = None
        
        # 6. Limpiar strings en 'descricao' (ANTES de enriquecer!)
        self.df['descricao'] = (self.df['descricao'].astype(str)
                                .str.replace(r"^Produto:\s*", "", regex=True)
                                .str.lstrip('- ')
                                .str.strip())

        # 7. Normalizar puntuación y espacios (Columnas A, D, F, G, H)
        indices_limpeza = [0, 3, 5, 6, 7]
        for idx in indices_limpeza:
            if idx < len(self.df.columns):
                col = self.df.columns[idx]
                self.df[col] = (self.df[col].astype(str)
                                .str.replace(',', '.')
                                .apply(lambda x: " ".join(x.split())))

        # 8. Formatear fechas
        self.df[nome_coluna_b] = self.df[nome_coluna_b].apply(self._formatar_data)
        
        # 9. Renombrar columnas finales
        if self.df.shape[1] == 10:
            self.df.columns = ['descricao', 'data_venda', 'pedido', 'cliente', 'vendedor',
                               'valor_unitario', 'unidades', 'valor', 'categoria', 'calibre']
        else:
            print(f"⚠️ Aviso: DataFrame tem {self.df.shape[1]} colunas, esperado 10.")

    def _propagar_descripciones(self):
        for index, row in self.df.iterrows():
            if isinstance(row.iloc[1], str) and 'produto' in row.iloc[1].lower():
                self.df.at[index, 'descricao'] = row.iloc[1]
        
        produto_atual = None
        for index, row in self.df.iterrows():
            if pd.notna(row['descricao']) and row['descricao'] != '':
                produto_atual = row['descricao']
            elif produto_atual is not None:
                self.df.at[index, 'descricao'] = produto_atual

    def _formatar_data(self, valor):
        try:
            data = pd.to_datetime(valor, format='%d/%m/%Y', errors='raise')
            return data.strftime('%Y-%m-%d')
        except ValueError:
            try:
                data = pd.to_datetime(valor, dayfirst=True, errors='raise')
                return data.strftime('%Y-%m-%d')
            except ValueError:
                return str(valor).strip()

    def enriquecer_con_fuzzy(self, limite_similitud=70):
        if self.df is None:
            print("❌ DataFrame não disponível!")
            return
            
        print(f"🔍 Iniciando busca fuzzy (limite: {limite_similitud})...")
        matches_exatos = 0
        matches_fuzzy = 0
        sem_match = 0
        
        for index, row in self.df.iterrows():
            chave_buscada = str(row['descricao']).lower().strip()

            if chave_buscada in self.dicionario_padrao:
                self._asignar_valores(index, chave_buscada)
                matches_exatos += 1
            else:
                maior_sim, melhor_chave = 0, None
                for key_padrao in self.dicionario_padrao.keys():
                    similitud = fuzz.ratio(chave_buscada, key_padrao)
                    if similitud > maior_sim:
                        maior_sim, melhor_chave = similitud, key_padrao
                
                if maior_sim >= limite_similitud and melhor_chave:
                    self._asignar_valores(index, melhor_chave)
                    matches_fuzzy += 1
                else:
                    sem_match += 1
        
        print(f"✅ Matches exatos: {matches_exatos}")
        print(f"✅ Matches fuzzy: {matches_fuzzy}")
        print(f"⚠️ Sem match: {sem_match}")

    def enriquecer_clientes_procv(self):
        if self.df_clientes is None: 
            print("⚠️ Tabela de clientes não carregada!")
            return

        cols_datos = ['CNPJ/CPF', 'Telefones', 'E-mails']
        
        # 1. Primer Cruce: Por Razão Social
        df_rs = self.df_clientes[['Razão social'] + cols_datos].copy()
        self.df = pd.merge(self.df, df_rs, left_on='cliente', right_on='Razão social', how='left')

        # 2. Segundo Cruce: Por Nome Fantasia
        df_nf = self.df_clientes[['Nome fantasia'] + cols_datos].copy()
        df_nf.columns = ['Nome fantasia'] + [c + '_nf' for c in cols_datos]
        self.df = pd.merge(self.df, df_nf, left_on='cliente', right_on='Nome fantasia', how='left')

        # 3. Consolidar datos (usa _nf si el original es NaN)
        for c in cols_datos:
            self.df[c] = self.df[c].fillna(self.df[c + '_nf'])
            self.df.drop(columns=[c + '_nf'], inplace=True)

        # 4. Limpieza final
        self.df.drop(columns=['Razão social', 'Nome fantasia'], errors='ignore', inplace=True)
        print("✅ Datos de clientes actualizados (Razón Social + Nombre Fantasía).")

    def _asignar_valores(self, index, chave_ref):
        self.df.at[index, 'categoria'] = self.dicionario_padrao[chave_ref][0]
        self.df.at[index, 'calibre'] = self.dicionario_padrao[chave_ref][1]

    def guardar_resultado(self, arquivos_tratados):
        if self.df is None:
            print("❌ Nada para guardar!")
            return
            
        os.makedirs(arquivos_tratados, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        nombre_salida = os.path.join(arquivos_tratados, f'arquivo-{timestamp}.xlsx')
        self.df.to_excel(nombre_salida, index=False)
        print(f"✅ Éxito: Archivo guardado en {nombre_salida}")

    def mostrar_resumen(self):
        if self.df is None:
            print("❌ DataFrame não disponível!")
            return
            
        print("\n" + "="*60)
        print("RESUMEN DEL PROCESAMIENTO")
        print("="*60)
        print(f"Total de registros: {len(self.df)}")
        print(f"Columnas: {list(self.df.columns)}")
        print(f"\nPrimeras 5 filas:")
        print(self.df.head())
        print(f"\nÚltimas 5 filas:")
        print(self.df.tail())
        print("="*60 + "\n")


if __name__ == "__main__":
    BASE_PATH = "rutas/a/tus/archivos"  # Cambia esto a la ruta correcta

    procesador = ProcesadorPedidos(
        ruta_reporte=os.path.join(BASE_PATH, 'nombre archivo', 'nombre archivo'), #archivo de pedidos
        ruta_padrao=os.path.join(BASE_PATH, 'tabelas-padrão', 'Tabela_padrao_produtos.xlsx'), #archivos de referencia
        ruta_clientes=os.path.join(BASE_PATH, 'tabelas-padrão', 'archivo clientes'), #archivo de clientes
        ruta_salida=os.path.join(BASE_PATH, 'Final_Procesado')   
    )

    procesador.cargar_datos()
    procesador.limpiar_documento()
    procesador.enriquecer_con_fuzzy()
    procesador.enriquecer_clientes_procv()
    procesador.mostrar_resumen()
    procesador.guardar_resultado(procesador.ruta_salida)