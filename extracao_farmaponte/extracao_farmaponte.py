import asyncio
import aiohttp
import json
import csv
import time
from datetime import datetime
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin, urlparse
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

class FarmaponteScraper:
    """
    Async web scraper for Farmaponte e-commerce website.
    Uses asyncio and aiohttp for parallel processing to efficiently scrape product data.
    """
    
    def __init__(self, max_concurrent_requests: int = 10, delay_between_requests: float = 0.5):
        self.base_url = "https://www.farmaponte.com.br/"
        self.max_concurrent_requests = max_concurrent_requests
        self.delay_between_requests = delay_between_requests
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Categories with their page counts
        self.categorias = {
            "mamae-e-bebe": "69",  # categoria e n de paginas
            "dermocosmeticos": "40",
            "cuidados-diarios": "111",
            "suplementos": "9",
            "saude": "326",
            "beleza": "133"
        }

        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close_session()
    
    async def start_session(self):
        """Initialize aiohttp session with proper configuration"""
        import ssl
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            ssl=ssl_context  # Use the SSL context that ignores certificate verification
        )
        
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout,
            connector=connector
        )
        print("🚀 Session started successfully")
    
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            print("📝 Session closed")
    
    async def fetch_page(self, url: str, retries: int = 3) -> Optional[str]:
        """
        Fetch a single page with rate limiting and retry logic
        
        Args:
            url: URL to fetch
            retries: Number of retry attempts
            
        Returns:
            HTML content or None if failed
        """
        async with self.semaphore:
            for attempt in range(retries):
                try:
                    await asyncio.sleep(self.delay_between_requests)
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:  # Rate limited
                            wait_time = 2 ** attempt  # Exponential backoff
                            print(f"⏳ Rate limited, waiting {wait_time}s before retry")
                            await asyncio.sleep(wait_time)
                        else:
                            print(f"❌ HTTP {response.status} for {url}")
                            
                except asyncio.TimeoutError:
                    print(f"⏰ Timeout on attempt {attempt + 1} for {url}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        
                except Exception as e:
                    print(f"❌ Error fetching {url}: {str(e)}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)
            
            print(f"🚫 Failed to fetch {url} after {retries} attempts")
            return None
    
    def extract_product_links_from_page(self, html: str, base_url: str) -> Set[str]:
        """
        Extract product links from a category page
        
        Args:
            html: HTML content of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            Set of product URLs
        """
        soup = BeautifulSoup(html, 'lxml')
        product_links = set()
        
        # Method 1: Find all <a> tags with class "item-image"
        # Based on the provided HTML: <a href="/product-path/p" class="item-image">
        item_image_links = soup.find_all('a', class_="item-image")
        
        for link_tag in item_image_links:
            if link_tag.get('href'):
                href = link_tag.get('href')
                # Convert relative URLs to absolute URLs
                full_url = urljoin(base_url, href)
                product_links.add(full_url)
        
        # Method 2: Find all <a> tags with href ending in "/p" (product pages)
        # This catches links like: <a href="/dipirona-gts-10ml-ger/p">
        product_page_links = soup.find_all('a', href=lambda x: x and x.endswith('/p'))
        
        for link_tag in product_page_links:
            if link_tag.get('href'):
                href = link_tag.get('href')
                # Convert relative URLs to absolute URLs
                full_url = urljoin(base_url, href)
                product_links.add(full_url)
        
        return product_links
    
    async def get_category_info(self, category: str) -> tuple[int, int]:
        """
        Get the number of pages and products for a category
        
        Args:
            category: Category name (e.g., 'mamae-e-bebe')
            
        Returns:
            Tuple of (number_of_pages, number_of_products)
        """
        category_url = f"{self.base_url}{category}"
        print(f"🔍 Fetching category info from: {category_url}")
        
        html = await self.fetch_page(category_url)
        if not html:
            print(f"❌ Failed to fetch category page for {category}")
            return 0, 0
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Find all divs with class "text-center pt-3"
        info_divs = soup.find_all('div', class_='text-center pt-3')
        
        num_pages = 0
        num_products = 0
        
        for div in info_divs:
            text = div.get_text(strip=True)
            
            # Check for page info: "Página 1 de 40"
            if 'Página' in text and ' de ' in text:
                try:
                    # Extract number after "de"
                    parts = text.split(' de ')
                    if len(parts) > 1:
                        num_pages = int(parts[1])
                        print(f"📄 Found {num_pages} pages for {category}")
                except ValueError:
                    pass
            
            # Check for product count: "794 resultados"
            elif 'resultados' in text:
                try:
                    # Extract number before "resultados"
                    parts = text.split(' resultados')
                    if len(parts) > 0:
                        num_products = int(parts[0].strip())
                        print(f"📊 Found {num_products} products for {category}")
                except ValueError:
                    pass
        
        return num_pages, num_products
    
    async def scrape_category_pages(self, category: str, max_pages: int) -> Set[str]:
        """
        Scrape all pages of a category to collect product links
        
        Args:
            category: Category name (e.g., 'mamae-e-bebe')
            max_pages: Maximum number of pages to scrape
            
        Returns:
            Set of all product URLs found in the category
        """
        print(f"🔍 Starting to scrape category: {category} ({max_pages} pages)")
        
        # Generate all page URLs for this category
        page_urls = []
        for page_num in range(1, max_pages + 1):
            url = f"{self.base_url}{category}/p={page_num}?/"
            page_urls.append(url)
        
        # Fetch all pages concurrently
        tasks = [self.fetch_page(url) for url in page_urls]
        
        # Use tqdm for progress bar
        pages_html = []
        for result in tqdm(asyncio.as_completed(tasks), desc=f"Fetching {category} pages", total=len(tasks)):
            html = await result
            if html:
                pages_html.append(html)
        
        # Extract product links from all pages
        all_product_links = set()
        for html in pages_html:
            page_links = self.extract_product_links_from_page(html, self.base_url)
            all_product_links.update(page_links)
        
        print(f"✅ Found {len(all_product_links)} unique products in {category}")
        return all_product_links
    
    async def scrape_all_categories(self, categories: Optional[List[str]] = None) -> Dict[str, Set[str]]:
        """
        Scrape all specified categories to collect product links
        
        Args:
            categories: List of category names to scrape. If None, scrape all categories.
            
        Returns:
            Dictionary mapping category names to sets of product URLs
        """
        if categories is None:
            categories = list(self.categorias.keys())
        
        print(f"🎯 Scraping {len(categories)} categories: {', '.join(categories)}")
        
        category_results = {}
        total_expected_products = 0
        
        for category in categories:
            # Get dynamic category info
            num_pages, num_products = await self.get_category_info(category)
            
            if num_pages == 0:
                print(f"⚠️ No pages found for category '{category}', skipping")
                continue
            
            total_expected_products += num_products
            print(f"📋 Category '{category}': {num_pages} pages, expecting {num_products} products")
            
            product_links = await self.scrape_category_pages(category, num_pages)
            category_results[category] = product_links
            
            # Debug: Compare expected vs actual
            actual_products = len(product_links)
            if actual_products != num_products:
                print(f"⚠️ Product count mismatch for {category}: expected {num_products}, got {actual_products}")
            else:
                print(f"✅ Product count matches for {category}: {actual_products}")
        
        # Calculate totals
        total_actual_products = sum(len(links) for links in category_results.values())
        print(f"\n📊 SUMMARY:")
        print(f"   Expected total products: {total_expected_products}")
        print(f"   Actual total products: {total_actual_products}")
        print(f"   Difference: {total_actual_products - total_expected_products}")
        
        return category_results
    
    def extract_product_details(self, html: str, product_url: str) -> Dict[str, str]:
        """
        Extract product details from a product page
        
        Args:
            html: HTML content of the product page
            product_url: URL of the product page
            
        Returns:
            Dictionary containing product details
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Initialize product data with default values
        product_data = {
            'name': '',
            'brand': '',
            'code': '',
            'gtin': '',
            'discount': '',
            'unit_price': '',
            'discount_price': '',
            'pix_price': '',
            'pix_discount': '',
            'sub_category': '',
            'link': product_url
        }
        
        try:
            # Extract name: h1.name
            name_element = soup.find('h1', class_='name')
            if name_element:
                product_data['name'] = name_element.get_text(strip=True)
            
            # Extract brand: a.title_marca (note: underscore, not hyphen)
            brand_element = soup.find('a', class_='title_marca')
            if brand_element:
                product_data['brand'] = brand_element.get_text(strip=True)
            
            # If brand is empty, set it to "generico"
            if not product_data['brand']:
                product_data['brand'] = 'generico'
            
            # Extract GTIN from JSON-LD script
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_ld_scripts:
                try:
                    import json
                    json_data = json.loads(script.string.strip())
                    if isinstance(json_data, dict) and json_data.get('@type') == 'Product':
                        gtin = json_data.get('gtin13', '')
                        if gtin:
                            product_data['gtin'] = gtin
                            break
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            # Try to get a cleaner product link from the page itself
            # Look for links with /p pattern that might be cleaner
            clean_link_element = soup.find('a', href=lambda x: x and x.endswith('/p'))
            if clean_link_element and clean_link_element.get('href'):
                from urllib.parse import urljoin
                clean_href = clean_link_element.get('href')
                clean_url = urljoin('https://www.farmaponte.com.br/', clean_href)
                product_data['link'] = clean_url
            # If no clean link found, keep the original URL
            
            # Extract code: span.mr-3
            code_element = soup.find('span', class_='mr-3')
            if code_element:
                product_data['code'] = code_element.get_text(strip=True)
            
            # Extract discount: span.discount
            discount_element = soup.find('span', class_='discount')
            if discount_element:
                product_data['discount'] = discount_element.get_text(strip=True)
            
            # Extract unit_price: p.unit-price
            unit_price_element = soup.find('p', class_='unit-price')
            if unit_price_element:
                # Clean up the text by removing extra whitespace and newlines
                price_text = unit_price_element.get_text(strip=True)
                product_data['unit_price'] = ' '.join(price_text.split())
            
            # Extract discount_price: p.sale-price.money (both classes must be present)
            discount_price_element = soup.find('p', class_=['sale-price', 'money'])
            if not discount_price_element:
                # Try finding p with both classes using CSS selector
                discount_price_element = soup.select('p.sale-price.money')
                if discount_price_element:
                    discount_price_element = discount_price_element[0]
                else:
                    discount_price_element = None
            
            if discount_price_element:
                # Clean up the text by removing extra whitespace and newlines
                price_text = discount_price_element.get_text(strip=True)
                product_data['discount_price'] = ' '.join(price_text.split())
            
            # Extract PIX price and discount: p.seal-pix.sale-price.sale-price-pix.mb-0.money
            pix_element = soup.find('p', class_=['seal-pix', 'sale-price', 'sale-price-pix', 'mb-0', 'money'])
            if not pix_element:
                # Try CSS selector for multiple classes
                pix_elements = soup.select('p.seal-pix.sale-price.sale-price-pix.mb-0.money')
                if pix_elements:
                    pix_element = pix_elements[0]
            
            if pix_element:
                # Extract PIX price
                pix_price_text = pix_element.get_text(strip=True)
                product_data['pix_price'] = ' '.join(pix_price_text.split())
                
                # Extract PIX discount from data-discount attribute
                pix_discount_attr = pix_element.get('data-discount')
                if pix_discount_attr:
                    product_data['pix_discount'] = pix_discount_attr
            
            # Extract sub-category: "pr-0 mr-0"
            sub_category_element = soup.find(class_='pr-0 mr-0')
            if sub_category_element:
                product_data['sub_category'] = sub_category_element.get_text(strip=True)
        
        except Exception as e:
            print(f"⚠️ Error extracting details from {product_url}: {str(e)}")
        
        return product_data
    
    async def scrape_product_details(self, product_urls: List[str]) -> List[Dict[str, str]]:
        """
        Scrape details for a list of product URLs
        
        Args:
            product_urls: List of product URLs to scrape
            
        Returns:
            List of dictionaries containing product details
        """
        print(f"🔍 Starting to scrape details for {len(product_urls)} products")
        
        # Create tasks for fetching all product pages
        tasks = [self.fetch_page(url) for url in product_urls]
        
        # Fetch all pages with progress bar
        products_data = []
        
        # Use asyncio.gather to maintain order of results
        print(f"   Fetching {len(tasks)} product pages...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results in the same order as input URLs
        for i, (result, product_url) in enumerate(zip(results, product_urls)):
            if isinstance(result, Exception):
                print(f"⚠️ Error for {product_url}: {str(result)}")
                products_data.append({
                    'name': 'FAILED_TO_FETCH',
                    'brand': '',
                    'code': '',
                    'gtin': '',
                    'discount': '',
                    'unit_price': '',
                    'discount_price': '',
                    'pix_price': '',
                    'pix_discount': '',
                    'sub_category': '',
                    'link': product_url
                })
            elif result:  # HTML content
                product_details = self.extract_product_details(result, product_url)
                products_data.append(product_details)
            else:
                # Empty result
                products_data.append({
                    'name': 'FAILED_TO_FETCH',
                    'brand': '',
                    'code': '',
                    'gtin': '',
                    'discount': '',
                    'unit_price': '',
                    'discount_price': '',
                    'pix_price': '',
                    'pix_discount': '',
                    'sub_category': '',
                    'link': product_url
                })
        
        print(f"✅ Successfully scraped {len([p for p in products_data if p['name'] != 'FAILED_TO_FETCH'])} products")
        return products_data
    
    def save_to_json(self, data: List[Dict[str, str]], filename: str = None) -> str:
        """
        Save scraped data to JSON file
        
        Args:
            data: List of product dictionaries
            filename: Optional filename. If None, generates timestamp-based name
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"farmaponte_products_{timestamp}.json"
        
        filepath = Path(filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Data saved to JSON: {filepath.absolute()}")
            return str(filepath.absolute())
            
        except Exception as e:
            print(f"❌ Error saving to JSON: {str(e)}")
            raise
    
    def save_to_csv(self, data: List[Dict[str, str]], filename: str = None) -> str:
        """
        Save scraped data to CSV file
        
        Args:
            data: List of product dictionaries
            filename: Optional filename. If None, generates timestamp-based name
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"farmaponte_products_{timestamp}.csv"
        
        filepath = Path(filename)
        
        try:
            if not data:
                print("⚠️ No data to save to CSV")
                return str(filepath.absolute())
            
            # Define the desired column order
            fieldnames = [
                'name',
                'sub_category', 
                'brand',
                'unit_price',
                'discount',
                'discount_price',
                'pix_discount',
                'pix_price',
                'code',
                'gtin',
                'link'
            ]
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            
            print(f"💾 Data saved to CSV: {filepath.absolute()}")
            return str(filepath.absolute())
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {str(e)}")
            raise
    
    def save_category_links(self, category_results: Dict[str, Set[str]], filename: str = None) -> str:
        """
        Save category product links to JSON file
        
        Args:
            category_results: Dictionary mapping categories to product URL sets
            filename: Optional filename. If None, generates timestamp-based name
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"farmaponte_links_{timestamp}.json"
        
        filepath = Path(filename)
        
        try:
            # Convert sets to lists for JSON serialization
            serializable_data = {
                category: list(links) for category, links in category_results.items()
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(serializable_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Category links saved to: {filepath.absolute()}")
            return str(filepath.absolute())
            
        except Exception as e:
            print(f"❌ Error saving category links: {str(e)}")
            raise


async def main():
    """
    Main function to orchestrate the entire scraping process
    """
    print("🚀 Starting Farmaponte scraping process...")
    start_time = time.time()
    
    # Configuration
    MAX_CONCURRENT = 15  # Adjust based on your needs and server capacity
    DELAY_BETWEEN_REQUESTS = 0.3  # Seconds between requests
    
    async with FarmaponteScraper(
        max_concurrent_requests=MAX_CONCURRENT,
        delay_between_requests=DELAY_BETWEEN_REQUESTS
    ) as scraper:
        
        try:
            # Step 1: Scrape all categories to get product links
            print("\n" + "="*60)
            print("📋 STEP 1: Collecting product links from all categories")
            print("="*60)
            
            category_results = await scraper.scrape_all_categories()
            
            # Save category links for backup
            links_file = scraper.save_category_links(category_results)
            
            # Step 2: Combine all product links
            print("\n" + "="*60)
            print("🔗 STEP 2: Preparing product URLs for detail scraping")
            print("="*60)
            
            all_product_urls = set()
            for category, links in category_results.items():
                all_product_urls.update(links)
            
            print(f"📊 Total unique products to scrape: {len(all_product_urls)}")
            
            # Convert to list for indexing
            product_urls_list = list(all_product_urls)
            
            # Step 3: Scrape product details
            print("\n" + "="*60)
            print("🔍 STEP 3: Scraping detailed product information")
            print("="*60)
            
            # You can limit the number of products for testing
            # product_urls_list = product_urls_list[:50]  # Uncomment to test with first 50 products
            
            products_data = await scraper.scrape_product_details(product_urls_list)
            
            # Step 4: Save results
            print("\n" + "="*60)
            print("💾 STEP 4: Saving scraped data")
            print("="*60)
            
            # Save to both JSON and CSV
            json_file = scraper.save_to_json(products_data)
            csv_file = scraper.save_to_csv(products_data)
            
            # Step 5: Summary
            print("\n" + "="*60)
            print("📊 SCRAPING SUMMARY")
            print("="*60)
            
            end_time = time.time()
            duration = end_time - start_time
            
            successful_products = len([p for p in products_data if p['name'] != 'FAILED_TO_FETCH'])
            failed_products = len(products_data) - successful_products
            
            print(f"⏱️  Total time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
            print(f"✅ Successfully scraped: {successful_products} products")
            print(f"❌ Failed to scrape: {failed_products} products")
            print(f"📁 Files saved:")
            print(f"   - Product links: {links_file}")
            print(f"   - Product data (JSON): {json_file}")
            print(f"   - Product data (CSV): {csv_file}")
            
            if successful_products > 0:
                print(f"⚡ Average time per product: {duration/successful_products:.3f} seconds")
            
            print("\n🎉 Scraping completed successfully!")
            
        except KeyboardInterrupt:
            print("\n🛑 Scraping interrupted by user")
        except Exception as e:
            print(f"\n❌ An error occurred during scraping: {str(e)}")
            raise


async def scrape_specific_categories(categories: List[str]):
    """
    Scrape specific categories only
    
    Args:
        categories: List of category names to scrape
    """
    print(f"🎯 Starting scraping for specific categories: {', '.join(categories)}")
    
    async with FarmaponteScraper(max_concurrent_requests=10, delay_between_requests=0.5) as scraper:
        # Scrape only specified categories
        category_results = await scraper.scrape_all_categories(categories)
        
        # Combine all product links
        all_product_urls = set()
        for category, links in category_results.items():
            all_product_urls.update(links)
        
        product_urls_list = list(all_product_urls)
        print(f"📊 Total products to scrape: {len(product_urls_list)}")
        
        # Scrape product details
        products_data = await scraper.scrape_product_details(product_urls_list)
        
        # Save results with category prefix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        categories_str = "_".join(categories)
        
        scraper.save_to_json(products_data, f"farmaponte_{categories_str}_{timestamp}.json")
        scraper.save_to_csv(products_data, f"farmaponte_{categories_str}_{timestamp}.csv")


if __name__ == "__main__":
    print("🌟 Farmaponte Product Scraper")
    print("="*50)
    
    # Example usage:
    # Run the main scraping process for all categories
    asyncio.run(main())
    
    # Alternative: Scrape specific categories only
    # asyncio.run(scrape_specific_categories(["mamae-e-bebe", "suplementos"]))

