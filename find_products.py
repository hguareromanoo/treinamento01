#!/usr/bin/env python3
"""
Find the correct product link pattern by examining the page structure more carefully
"""

import asyncio
import aiohttp
import ssl
from bs4 import BeautifulSoup
import re

class ProductLinkFinder:
    def __init__(self):
        # SSL context to bypass certificate verification issues
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        self.base_url = "https://www.farmaponte.com.br"
    
    async def find_product_structure(self, session, category, page):
        """Find the actual product structure on the page"""
        try:
            url = f"{self.base_url}/{category}/p={page}?/"
            async with session.get(url, ssl=self.ssl_context) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    print(f"\n=== Analyzing {category} page {page} ===")
                    
                    # Method 1: Look for links ending with "/p"
                    links_ending_p = []
                    all_links = soup.find_all('a', href=True)
                    
                    for link in all_links:
                        href = link.get('href', '')
                        if href.endswith('/p'):
                            if href.startswith('/'):
                                full_url = self.base_url + href
                            else:
                                full_url = href
                            links_ending_p.append(full_url)
                    
                    print(f"Links ending with '/p': {len(links_ending_p)}")
                    if links_ending_p:
                        print("Examples:")
                        for i, link in enumerate(links_ending_p[:5]):
                            print(f"  {i+1}. {link}")
                    
                    # Method 2: Look for product containers/cards
                    # Common selectors for product items
                    product_selectors = [
                        '.product-item',
                        '.item-product',
                        '.product-card',
                        '.product',
                        '[class*="product"]',
                        '[class*="item"]'
                    ]
                    
                    for selector in product_selectors:
                        elements = soup.select(selector)
                        if elements:
                            print(f"\nFound {len(elements)} elements with selector '{selector}'")
                            # Look for links within these elements
                            product_links = []
                            for element in elements[:3]:  # Check first 3 elements
                                links = element.find_all('a', href=True)
                                for link in links:
                                    href = link.get('href', '')
                                    if href and ('/p' in href or href.endswith('/p')):
                                        if href.startswith('/'):
                                            full_url = self.base_url + href
                                        else:
                                            full_url = href
                                        product_links.append(full_url)
                            
                            if product_links:
                                print(f"  Product links found: {len(product_links)}")
                                for i, link in enumerate(product_links[:3]):
                                    print(f"    {i+1}. {link}")
                    
                    # Method 3: Look at the page source for product listing patterns
                    # Find script tags or data attributes that might contain product info
                    scripts = soup.find_all('script')
                    for script in scripts:
                        if script.string and ('product' in script.string.lower() or 'item' in script.string.lower()):
                            content = script.string[:200]  # First 200 chars
                            if 'http' in content:
                                print(f"\nFound script with product references:")
                                print(f"  {content}...")
                                break
                    
                    # Method 4: Look for specific patterns in the HTML
                    # Check for image containers that might link to products
                    img_containers = soup.find_all(['div', 'figure', 'a'], class_=re.compile(r'.*image.*', re.I))
                    product_image_links = []
                    
                    for container in img_containers:
                        # Look for links within or on the container
                        if container.name == 'a' and container.get('href'):
                            href = container.get('href', '')
                            if '/p' in href or href.endswith('/p'):
                                if href.startswith('/'):
                                    full_url = self.base_url + href
                                else:
                                    full_url = href
                                product_image_links.append(full_url)
                        else:
                            links = container.find_all('a', href=True)
                            for link in links:
                                href = link.get('href', '')
                                if '/p' in href or href.endswith('/p'):
                                    if href.startswith('/'):
                                        full_url = self.base_url + href
                                    else:
                                        full_url = href
                                    product_image_links.append(full_url)
                    
                    if product_image_links:
                        print(f"\nProduct image links: {len(set(product_image_links))}")
                        for i, link in enumerate(list(set(product_image_links))[:5]):
                            print(f"  {i+1}. {link}")
                    
                    return len(set(links_ending_p))
                    
        except Exception as e:
            print(f"Error analyzing {category} page {page}: {e}")
            return 0
    
    async def run_analysis(self):
        """Run analysis on a few categories"""
        categories_to_test = ["medicamentos", "beleza"]
        
        async with aiohttp.ClientSession() as session:
            for category in categories_to_test:
                count = await self.find_product_structure(session, category, 1)
                print(f"\nSummary for {category}: {count} potential product links")
                await asyncio.sleep(1)

async def main():
    finder = ProductLinkFinder()
    await finder.run_analysis()

if __name__ == "__main__":
    asyncio.run(main())