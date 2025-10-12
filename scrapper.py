import requests
from bs4 import BeautifulSoup
import pandas as pd 
import time
import urllib.parse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re
import threading
import asyncio
import aiohttp


class VeraCruzScraper:
    def __init__(self):
        self.url = "https://www.drogariaveracruz.com.br/medicamentos/"
        self.url_base = self.url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def achar_nome(self, div):
        tag_h2 = div.find('h2', class_='title')
        if tag_h2:
            tag_a = tag_h2.find('a')
            if tag_a:
                return tag_a.get_text(strip=True)
        return "Nome não encontrado"

    def achar_preco(self, div):
        tag_preco = div.find('p', class_="unit-price p-0")
        if tag_preco:
            return tag_preco.get_text().strip()
        return None

    def achar_precopix(self, div):
        tag_precopix = div.find('p', class_="sale-price-pix")
        if tag_precopix:
            strong_tag = tag_precopix.find('strong')
            if strong_tag:
                return strong_tag.get_text(strip=True)
        return None 

    def achar_total_paginas(self, soup_inicial):
        container_template = soup_inicial.find('div', class_='page-template')
        if container_template:
            divs_candidatas = container_template.find_all('div', class_='text-center pt-3')
            for div in divs_candidatas:
                texto = div.get_text(strip=True)
                if "Página" in texto:
                    partes = texto.split()
                    ultima_palavra = partes[-1]
                    if ultima_palavra.isdigit():
                        return int(ultima_palavra)
        return 1

    def achar_link(self, produto_html, url_base):
        tag_h2 = produto_html.find('h2', class_='title')
        if tag_h2:
            tag_a = tag_h2.find('a')
            if tag_a and 'href' in tag_a.attrs:
                link_relativo = tag_a.get('href')
                return urllib.parse.urljoin(url_base, link_relativo)
        return "Não encontrado"

    def achar_precodesconto(self, produto_html):
        tag_venda = produto_html.find('p', class_="sale-price p-0")
        if tag_venda:
            strong_tag = tag_venda.find('strong')
            if strong_tag:
                return strong_tag.get_text(strip=True)
        return None

    def extrair_detalhes_do_json(self, soup_detalhes):
        brand = "Não encontrada"
        code = "Não encontrado"
        script_tag = soup_detalhes.find('script', type='application/ld+json')
        if script_tag:
            try:
                raw_json = script_tag.string.strip()
                raw_json = raw_json.replace("\n", "").replace("\r", "").replace("\t", "")
                dados_json = json.loads(raw_json)
                if isinstance(dados_json, list):
                    for item in dados_json:
                        if isinstance(item, dict) and 'brand' in item:
                            dados_json = item
                            break
                if 'brand' in dados_json and isinstance(dados_json['brand'], dict):
                    brand = dados_json['brand'].get('name', "Não encontrada")
                code = dados_json.get('gtin13', "Não encontrado")
            except Exception as e:
                print("  -> Aviso: Erro ao ler JSON-LD:", e)
        return brand, code

    def limpar_preco(self, preco_str):
        if not preco_str: return None
        try:
            return float(preco_str.replace("R$", "").strip().replace('.', '').replace(',', '.'))
        except (ValueError, TypeError):
            return None

    def baixar_url(self, url, tentativas=3):
        for i in range(tentativas):
            try:
                r = self.session.get(url, timeout=20)
                if r.status_code == 200:
                    return r
            except Exception:
                time.sleep(random.uniform(0.5, 2))
        return None

    def processar_produto(self, produto, url_base):
        nome = self.achar_nome(produto)
        preco = self.achar_preco(produto)
        precopix = self.achar_precopix(produto)
        precodesconto = self.achar_precodesconto(produto)
        link_produto = self.achar_link(produto, url_base)

        brand, code = None, None
        if link_produto != "Não encontrado":
            responseprodutos = self.baixar_url(link_produto)
            if responseprodutos:
                soup_produto = BeautifulSoup(responseprodutos.content, 'html.parser')
                brand, code = self.extrair_detalhes_do_json(soup_produto)

        unit_price = self.limpar_preco(preco)
        discount_price = self.limpar_preco(precodesconto)
        pix_price = self.limpar_preco(precopix)

        discount = round(unit_price - discount_price, 2) if unit_price and discount_price else None
        pix_discount = round(unit_price - pix_price, 2) if unit_price and pix_price else None

        return {
            'Marca': brand,
            'GTIN': code,
            'Desconto': discount,
            'Preco_com_desconto': discount_price,
            'Link': link_produto,
            'Nome': nome,
            'Desconto_com_pix': pix_discount,
            'Preco_pix': pix_price,
            'Preco_unitario': unit_price
        }

    def processar_pagina(self, pagina, url_base):
        url_pagina = f'https://www.drogariaveracruz.com.br/medicamentos/?p={pagina}'
        response = self.baixar_url(url_pagina)
        if not response:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        div_produtos = soup.find_all('div', class_='li')
        resultados = []

        # processamento paralelo de produtos
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(self.processar_produto, produto, url_base) for produto in div_produtos]
            for future in as_completed(futures):
                resultado = future.result()
                if resultado:
                    resultados.append(resultado)
                    print(f"Extraído (pág. {pagina}): {resultado['Nome']}")
        return resultados

    async def scrape(self, output_file='veracruz-final.csv'):
        """Main method to execute the scraping process"""
        # --- PRIMEIRA PÁGINA para descobrir total ---
        response = self.baixar_url(self.url)
        soup = BeautifulSoup(response.content, 'html.parser')
        total_paginas = self.achar_total_paginas(soup)

        lista_de_produtos = []

        # --- NOVO: paralelismo também entre páginas ---
        print(f"Iniciando extração em {total_paginas} páginas...\n")
        with ThreadPoolExecutor(max_workers=5) as executor_paginas:  # controla quantas páginas em paralelo
            futures_paginas = [executor_paginas.submit(self.processar_pagina, pagina, self.url_base) for pagina in range(1, total_paginas + 1)]
            for future in as_completed(futures_paginas):
                resultados_pagina = future.result()
                lista_de_produtos.extend(resultados_pagina)

        # --- FINAL ---
        print("\nExtração concluída! Gerando tabela...")
        df = pd.DataFrame(lista_de_produtos)

        df.to_csv(output_file, index=False)
        print(f"Arquivo salvo: {output_file} ✅")
        
        return df


class FarmaponteScraper:
    def __init__(self):
        self.url = "https://www.farmaponte.com.br/saude/"
        self.url_base = self.url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.8,en;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Criar sessão com pool de conexões otimizado
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,  # Mais conexões simultâneas
            pool_maxsize=50,     # Pool maior
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Controle de rate limiting inteligente
        self.last_request_time = 0
        self.request_lock = threading.Lock()
        self.consecutive_errors = 0
        
        # Cache para evitar requisições duplicadas
        self.url_cache = {}
        self.cache_lock = threading.Lock()

    def achar_nome(self, div):
        tag_h2 = div.find('h2', class_='title')
        if tag_h2:
            tag_a = tag_h2.find('a')
            if tag_a:
                return tag_a.get_text(strip=True)
        return "Nome não encontrado"

    def achar_preco(self, div):
        """Busca o preço unitário (preço original)"""
        tag_preco = div.find('p', class_="unit-price")
        if tag_preco:
            return tag_preco.get_text().strip()
        return None

    def achar_precopix(self, div):
        """OTIMIZADO: Busca o preço PIX com método mais eficiente"""
        # Método mais direto primeiro
        for class_name in ['seal-pix sale-price sale-price-pix mb-0 money', 'seal-pix']:
            tag_pix = div.find('p', class_=class_name)
            if tag_pix:
                return tag_pix.get_text().strip()
        return None 

    def achar_total_paginas(self, soup_inicial):
        container_template = soup_inicial.find('div', class_='page-template')
        if container_template:
            divs_candidatas = container_template.find_all('div', class_='text-center pt-3')
            for div in divs_candidatas:
                texto = div.get_text(strip=True)
                if "Página" in texto:
                    # Regex mais eficiente
                    match = re.search(r'Página\s+\d+\s+de\s+(\d+)', texto)
                    if match:
                        return int(match.group(1))
        return 1

    def achar_link(self, produto_html, url_base):
        tag_h2 = produto_html.find('h2', class_='title')
        if tag_h2:
            tag_a = tag_h2.find('a')
            if tag_a and 'href' in tag_a.attrs:
                link_relativo = tag_a.get('href')
                return urllib.parse.urljoin(url_base, link_relativo)
        return None

    def achar_precodesconto(self, produto_html):
        """OTIMIZADO: Busca o preço com desconto de forma mais eficiente"""
        # Método mais direto
        tag_venda = produto_html.find('p', class_="sale-price money")
        if tag_venda:
            strong_tag = tag_venda.find('strong')
            return (strong_tag or tag_venda).get_text(strip=True)
        return None

    def achar_desconto_percentual(self, produto_html):
        """OTIMIZADO: Busca percentual de desconto"""
        discount_element = produto_html.find('span', class_='discount')
        if discount_element:
            return discount_element.get_text(strip=True)
        return None

    def limpar_json_string(self, json_string):
        """OTIMIZADO: Limpeza mais eficiente de JSON"""
        if not json_string:
            return json_string
        
        # Operações mais eficientes em uma só passada
        json_string = re.sub(r'[\n\r\t]+', '', json_string)
        json_string = re.sub(r'\s{2,}', ' ', json_string)
        json_string = re.sub(r',,+', ',', json_string)
        json_string = re.sub(r',\s*([}\]])', r'\1', json_string)
        
        return json_string.strip()

    def extrair_detalhes_do_json(self, soup_detalhes):
        """OTIMIZADO: Extração mais rápida do JSON-LD"""
        brand = None
        code = None
        
        # Buscar apenas o primeiro script JSON-LD relevante
        script_tag = soup_detalhes.find('script', type='application/ld+json')
        if not script_tag or not script_tag.string:
            return brand, code
                
        try:
            raw_json = script_tag.string.strip()
            cleaned_json = self.limpar_json_string(raw_json)
            dados_json = json.loads(cleaned_json)
            
            # Tratamento simplificado
            if isinstance(dados_json, list) and dados_json:
                for item in dados_json:
                    if isinstance(item, dict) and ('brand' in item or 'gtin13' in item):
                        dados_json = item
                        break
            
            # Extração direta
            if isinstance(dados_json, dict):
                # Marca
                brand_data = dados_json.get('brand')
                if isinstance(brand_data, dict):
                    brand = brand_data.get('name')
                elif isinstance(brand_data, str):
                    brand = brand_data
                
                # Código
                code = dados_json.get('gtin13') or dados_json.get('gtin')
                if code:
                    code = str(code)
                    
        except:
            # Fallback com regex mais simples
            try:
                raw_json = script_tag.string
                brand_match = re.search(r'"brand":\s*(?:{\s*"name":\s*"([^"]+)"|"([^"]+)")', raw_json)
                if brand_match:
                    brand = brand_match.group(1) or brand_match.group(2)
                
                gtin_match = re.search(r'"gtin(?:13)?":\s*"([^"]+)"', raw_json)
                if gtin_match:
                    code = gtin_match.group(1)
            except:
                pass
        
        return brand, code

    def extrair_detalhes_adicionais_da_pagina(self, soup_detalhes):
        """OTIMIZADO: Extração mais rápida de detalhes"""
        detalhes = {}
        
        # Buscas mais diretas
        elements = {
            'unit_price_detailed': soup_detalhes.find('p', class_='unit-price'),
            'discount_price_detailed': soup_detalhes.select_one('p.sale-price.money'),
            'pix_price_detailed': soup_detalhes.select_one('p.seal-pix.sale-price.sale-price-pix.mb-0.money')
        }
        
        for key, element in elements.items():
            if element:
                if key == 'pix_price_detailed':
                    detalhes[key] = element.get_text(strip=True)
                    pix_discount = element.get('data-discount')
                    if pix_discount:
                        detalhes['pix_discount_percent'] = pix_discount
                else:
                    detalhes[key] = element.get_text(strip=True)
            else:
                detalhes[key] = None
        
        return detalhes

    def limpar_preco(self, preco_str):
        """OTIMIZADO: Limpeza mais rápida de preços"""
        if not preco_str: 
            return None
        try:
            # Regex mais eficiente para extrair números
            match = re.search(r'(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)', preco_str.replace("R$", ""))
            if match:
                preco_limpo = match.group(1).replace('.', '').replace(',', '.')
                return float(preco_limpo)
        except:
            pass
        return None

    def baixar_url(self, url, tentativas=2):
        """OTIMIZADO: Download mais inteligente com rate limiting adaptativo"""
        # Cache check
        with self.cache_lock:
            if url in self.url_cache:
                return self.url_cache[url]
        
        for i in range(tentativas):
            try:
                # Rate limiting inteligente
                with self.request_lock:
                    current_time = time.time()
                    time_since_last = current_time - self.last_request_time
                    
                    # Delay adaptativo baseado em erros
                    if self.consecutive_errors > 0:
                        delay = min(0.5 + (self.consecutive_errors * 0.2), 2.0)
                    else:
                        delay = 0.1  # Delay mínimo quando tudo está funcionando
                    
                    if time_since_last < delay:
                        time.sleep(delay - time_since_last)
                    
                    self.last_request_time = time.time()
                
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    self.consecutive_errors = 0
                    # Cache response
                    with self.cache_lock:
                        self.url_cache[url] = response
                    return response
                elif response.status_code == 429:
                    self.consecutive_errors += 1
                    time.sleep(min(2 ** i, 10))  # Exponential backoff
                else:
                    self.consecutive_errors += 1
                    
            except Exception as e:
                self.consecutive_errors += 1
                if i == tentativas - 1:
                    print(f"⚠️ Falha persistente: {str(e)[:50]}...")
                time.sleep(0.5 * (i + 1))
        
        return None

    def processar_produto_rapido(self, produto, url_base):
        """OTIMIZADO: Processamento mais rápido de produto"""
        nome = self.achar_nome(produto)
        preco = self.achar_preco(produto)
        precopix = self.achar_precopix(produto)
        precodesconto = self.achar_precodesconto(produto)
        desconto_percentual = self.achar_desconto_percentual(produto)
        link_produto = self.achar_link(produto, url_base)

        brand, code = None, None
        detalhes_extras = {}
        
        # Apenas processar se o link for válido
        if link_produto:
            responseprodutos = self.baixar_url(link_produto)
            if responseprodutos:
                soup_produto = BeautifulSoup(responseprodutos.content, 'html.parser')
                brand, code = self.extrair_detalhes_do_json(soup_produto)
                detalhes_extras = self.extrair_detalhes_adicionais_da_pagina(soup_produto)

        # Usar preços da página do produto se disponíveis
        preco_final = detalhes_extras.get('unit_price_detailed') or preco
        precodesconto_final = detalhes_extras.get('discount_price_detailed') or precodesconto
        precopix_final = detalhes_extras.get('pix_price_detailed') or precopix

        unit_price = self.limpar_preco(preco_final)
        discount_price = self.limpar_preco(precodesconto_final)
        pix_price = self.limpar_preco(precopix_final)

        # Calcular descontos
        discount = round(unit_price - discount_price, 2) if unit_price and discount_price else None
        pix_discount = round(unit_price - pix_price, 2) if unit_price and pix_price else None

        return {
            'Nome': nome,
            'Marca': brand,
            'GTIN': code,
            'Preco_unitario': unit_price,
            'Preco_com_desconto': discount_price,
            'Preco_pix': pix_price,
            'Desconto': discount,
            'Desconto_com_pix': pix_discount,
            'Link': link_produto
        }

    def processar_pagina_completa(self, pagina_info):
        """NOVA FUNÇÃO: Processa uma página completa de forma otimizada"""
        pagina, total_paginas, url_base = pagina_info
        
        print(f"📄 Processando página {pagina}/{total_paginas}")
        url_pagina = f'https://www.farmaponte.com.br/saude/?p={pagina}'
        
        response = self.baixar_url(url_pagina)
        if not response:
            print(f"❌ Falha ao carregar página {pagina}")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        div_produtos = soup.find_all('div', class_='li')
        print(f"🔍 Página {pagina}: {len(div_produtos)} produtos encontrados")

        # Processar produtos da página com mais workers
        produtos_pagina = []
        with ThreadPoolExecutor(max_workers=8) as executor:  # Aumentado para 8
            futures = [executor.submit(self.processar_produto_rapido, produto, url_base) 
                      for produto in div_produtos]
            
            for future in as_completed(futures):
                resultado = future.result()
                if resultado:
                    produtos_pagina.append(resultado)

        print(f"✅ Página {pagina}: {len(produtos_pagina)} produtos processados")
        return produtos_pagina

    async def scrape(self, output_file='farmaponte_otimizado.csv', max_paginas=None):
        """Main method to execute the scraping process"""
        start_time = time.time()

        # --- PRIMEIRA PÁGINA para descobrir total ---
        print("🔍 Descobrindo total de páginas...")
        response = self.baixar_url(self.url)
        soup = BeautifulSoup(response.content, 'html.parser')
        total_paginas = self.achar_total_paginas(soup)
        print(f"📊 Total de páginas encontradas: {total_paginas}")

        # Preparar lista de páginas para processamento paralelo
        if max_paginas is None:
            max_paginas = total_paginas
        else:
            max_paginas = min(max_paginas, total_paginas)
            
        paginas_info = [(p, total_paginas, self.url_base) for p in range(1, max_paginas + 1)]

        lista_de_produtos = []

        print(f"🚀 Iniciando extração paralela de {max_paginas} páginas...")
        print(f"⏱️  Tempo de início: {time.strftime('%H:%M:%S')}")

        # PROCESSAMENTO PARALELO DE PÁGINAS
        with ThreadPoolExecutor(max_workers=5) as page_executor:  # 4 páginas em paralelo
            page_futures = [page_executor.submit(self.processar_pagina_completa, info) 
                           for info in paginas_info]
            
            for future in as_completed(page_futures):
                produtos_pagina = future.result()
                lista_de_produtos.extend(produtos_pagina)
                print(f"📊 Total acumulado: {len(lista_de_produtos)} produtos")

        # Calcular tempo de execução
        end_time = time.time()
        tempo_execucao = end_time - start_time

        # --- RESULTADOS FINAIS ---
        print(f"\n🎉 Extração concluída em {tempo_execucao:.1f} segundos!")
        print(f"📊 Coletados {len(lista_de_produtos)} produtos")
        print(f"⚡ Velocidade: {len(lista_de_produtos)/tempo_execucao:.1f} produtos/segundo")

        print("\n📊 Gerando tabela...")
        df = pd.DataFrame(lista_de_produtos)

        print("💾 Salvando arquivo CSV...")
        df.to_csv(output_file, index=False)
        print(f"✅ Arquivo salvo como '{output_file}'")

        # Estatísticas de qualidade
        print(f"\n📈 ESTATÍSTICAS DE QUALIDADE:")
        print(f"Total de produtos: {len(df)}")
        print(f"Produtos com preço unitário: {df['Preco_unitario'].notna().sum()}")
        print(f"Produtos com preço desconto: {df['Preco_com_desconto'].notna().sum()}")
        print(f"Produtos com preço PIX: {df['Preco_pix'].notna().sum()}")
        print(f"Produtos com marca: {df['Marca'].notna().sum()}")
        print(f"Produtos com GTIN: {df['GTIN'].notna().sum()}")

        # Estatísticas de performance
        print(f"\n⚡ ESTATÍSTICAS DE PERFORMANCE:")
        print(f"Tempo total: {tempo_execucao:.1f}s")
        print(f"Páginas processadas: {max_paginas}")
        print(f"Produtos por página (média): {len(lista_de_produtos)/max_paginas:.1f}")
        print(f"Tempo por página (média): {tempo_execucao/max_paginas:.1f}s")
        print(f"Requests em cache: {len(self.url_cache)}")
        print(f"Taxa de erro final: {self.consecutive_errors}")

        # Exemplo de produto completo
        produtos_completos = df[(df['Preco_unitario'].notna()) & 
                               (df['Marca'].notna()) & 
                               (df['GTIN'].notna())]

        if len(produtos_completos) > 0:
            print(f"\n📋 Exemplo de produto com dados completos:")
            exemplo = produtos_completos.iloc[0]
            for coluna, valor in exemplo.items():
                if valor is not None and str(valor) != 'nan':
                    print(f"  {coluna}: {valor}")
        else:
            print(f"\n📋 Exemplo de produto coletado:")
            if len(df) > 0:
                exemplo = df.iloc[0]
                for coluna, valor in exemplo.items():
                    if valor is not None and str(valor) != 'nan':
                        print(f"  {coluna}: {valor}")

        return df

