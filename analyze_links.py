#!/usr/bin/env python3
"""
Analyze the types of links being extracted to identify the issue
"""

import asyncio
import aiohttp
import ssl
from bs4 import BeautifulSoup
from collections import Counter
import re

class LinkAnalyzer:
    def __init__(self):
        # SSL context to bypass certificate verification issues
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        self.base_url = "https://www.farmaponte.com.br"
    
    async def analyze_links_on_page(self, session, category, page):
        """Analyze all links on a page to understand what we're extracting"""
        try:
            url = f"{self.base_url}/{category}/p={page}?/"
            async with session.get(url, ssl=self.ssl_context) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Get all links with "/p" in href
                    all_links = soup.find_all('a', href=lambda x: x and '/p' in x)
                    
                    link_patterns = []
                    unique_links = set()
                    
                    print(f"\n=== Page {page} of {category} ===")
                    print(f"Total links with '/p': {len(all_links)}")
                    
                    # Analyze patterns
                    for link in all_links:
                        href = link.get('href', '')
                        if href:
                            # Clean up the href
                            if href.startswith('/'):
                                full_url = self.base_url + href
                            else:
                                full_url = href
                            
                            unique_links.add(full_url)
                            
                            # Extract pattern
                            # Look for common patterns
                            if '/p=' in href:
                                link_patterns.append('pagination')
                            elif href.count('/p') > 1:
                                link_patterns.append('multiple_p')
                            elif re.search(r'/p$', href):
                                link_patterns.append('ends_with_p')
                            elif re.search(r'/p[^a-zA-Z]', href):
                                link_patterns.append('p_with_separator')
                            else:
                                link_patterns.append('other_p')
                    
                    print(f"Unique links: {len(unique_links)}")
                    
                    # Show pattern distribution
                    pattern_counts = Counter(link_patterns)
                    print("Link patterns:")
                    for pattern, count in pattern_counts.most_common():
                        print(f"  {pattern}: {count}")
                    
                    # Show some example links
                    print("\nExample links (first 10):")
                    for i, link in enumerate(list(unique_links)[:10]):
                        print(f"  {i+1}. {link}")
                    
                    # Try to identify actual product links
                    product_links = set()
                    
                    # Look for specific product patterns
                    for link in unique_links:
                        # Product links typically have a specific pattern
                        if re.search(r'/[^/]+-p-\d+', link):  # e.g., /produto-name-p-12345
                            product_links.add(link)
                        elif re.search(r'/p/[^/]+', link):  # e.g., /p/produto-name
                            product_links.add(link)
                    
                    print(f"\nPotential product links: {len(product_links)}")
                    if product_links:
                        print("Product link examples:")
                        for i, link in enumerate(list(product_links)[:5]):
                            print(f"  {i+1}. {link}")
                    
                    return len(all_links), len(unique_links), len(product_links)
                    
        except Exception as e:
            print(f"Error analyzing {category} page {page}: {e}")
            return 0, 0, 0
    
    async def run_analysis(self):
        """Run analysis on a few categories"""
        categories_to_test = ["medicamentos", "beleza"]  # Test smaller categories first
        
        async with aiohttp.ClientSession() as session:
            for category in categories_to_test:
                await self.analyze_links_on_page(session, category, 1)
                await asyncio.sleep(1)

async def main():
    analyzer = LinkAnalyzer()
    await analyzer.run_analysis()

if __name__ == "__main__":
    asyncio.run(main())