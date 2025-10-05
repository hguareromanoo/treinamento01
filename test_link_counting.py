#!/usr/bin/env python3
"""
Test script to analyze link extraction and product counting
"""

import asyncio
import aiohttp
import ssl
from bs4 import BeautifulSoup
import time

class LinkCountTester:
    def __init__(self):
        # SSL context to bypass certificate verification issues
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        self.base_url = "https://www.farmaponte.com.br"
        self.categories = [
            "medicamentos",
            "dermocosmeticos", 
            "suplementos",
            "beleza",
            "higiene-e-cuidado-pessoal",
            "mamae-e-bebe",
            "casa-e-familia"
        ]
    
    async def get_category_info(self, session, category):
        """Get the total pages and products for a category"""
        try:
            url = f"{self.base_url}/{category}/p=1?/"
            async with session.get(url, ssl=self.ssl_context) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Find pagination info - look for "Página X de Y"
                    total_pages = 1
                    pagination_elements = soup.find_all(string=lambda text: text and "Página" in text and "de" in text)
                    for element in pagination_elements:
                        if "Página" in element and "de" in element:
                            try:
                                parts = element.strip().split()
                                if len(parts) >= 4 and parts[0] == "Página" and parts[2] == "de":
                                    total_pages = int(parts[3])
                                    break
                            except (ValueError, IndexError):
                                continue
                    
                    # Find total products - look for "X resultados"
                    total_products = 0
                    result_elements = soup.find_all(string=lambda text: text and "resultados" in text)
                    for element in result_elements:
                        if "resultados" in element:
                            try:
                                # Extract number from "794 resultados" format
                                number_str = element.strip().split()[0]
                                total_products = int(number_str)
                                break
                            except (ValueError, IndexError):
                                continue
                    
                    return total_pages, total_products
                else:
                    print(f"Failed to fetch {category}: HTTP {response.status}")
                    return 1, 0
        except Exception as e:
            print(f"Error getting info for {category}: {e}")
            return 1, 0
    
    async def count_links_on_page(self, session, category, page):
        """Count product links on a specific page"""
        try:
            url = f"{self.base_url}/{category}/p={page}?/"
            async with session.get(url, ssl=self.ssl_context) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Method 1: Look for links with "/p" in href
                    method1_links = soup.find_all('a', href=lambda x: x and '/p' in x)
                    
                    # Method 2: Look for elements with "item-image" class
                    method2_elements = soup.find_all(class_="item-image")
                    method2_links = []
                    for element in method2_elements:
                        link = element.find('a', href=True)
                        if link:
                            method2_links.append(link)
                    
                    # Method 3: Combined approach (what the scraper uses)
                    combined_links = set()
                    
                    # Add links from method 1
                    for link in method1_links:
                        href = link.get('href', '')
                        if href and '/p' in href:
                            if href.startswith('/'):
                                full_url = self.base_url + href
                            else:
                                full_url = href
                            combined_links.add(full_url)
                    
                    # Add links from method 2
                    for link in method2_links:
                        href = link.get('href', '')
                        if href:
                            if href.startswith('/'):
                                full_url = self.base_url + href
                            else:
                                full_url = href
                            combined_links.add(full_url)
                    
                    return len(method1_links), len(method2_links), len(combined_links)
                else:
                    return 0, 0, 0
        except Exception as e:
            print(f"Error counting links on {category} page {page}: {e}")
            return 0, 0, 0
    
    async def test_category(self, session, category):
        """Test link counting for a specific category"""
        print(f"\n=== Testing Category: {category} ===")
        
        # Get category info
        total_pages, total_products = await self.get_category_info(session, category)
        print(f"Total pages detected: {total_pages}")
        print(f"Total products expected: {total_products}")
        
        # Test first few pages
        pages_to_test = min(3, total_pages)
        total_method1 = 0
        total_method2 = 0
        total_combined = 0
        
        for page in range(1, pages_to_test + 1):
            method1, method2, combined = await self.count_links_on_page(session, category, page)
            print(f"Page {page}: Method1={method1}, Method2={method2}, Combined={combined}")
            total_method1 += method1
            total_method2 += method2
            total_combined += combined
            
            # Small delay between requests
            await asyncio.sleep(0.5)
        
        # Estimate total links based on sample
        if pages_to_test > 0:
            avg_combined = total_combined / pages_to_test
            estimated_total_links = int(avg_combined * total_pages)
            
            print(f"\nSummary for {category}:")
            print(f"  Average links per page (combined method): {avg_combined:.1f}")
            print(f"  Estimated total links: {estimated_total_links}")
            print(f"  Expected products: {total_products}")
            print(f"  Difference: {estimated_total_links - total_products}")
            
            return {
                'category': category,
                'total_pages': total_pages,
                'expected_products': total_products,
                'estimated_links': estimated_total_links,
                'avg_links_per_page': avg_combined,
                'difference': estimated_total_links - total_products
            }
        
        return None
    
    async def run_test(self):
        """Run the complete test"""
        print("Starting link counting test...")
        print(f"Testing categories: {', '.join(self.categories)}")
        
        results = []
        
        async with aiohttp.ClientSession() as session:
            for category in self.categories:
                result = await self.test_category(session, category)
                if result:
                    results.append(result)
                
                # Delay between categories
                await asyncio.sleep(1)
        
        # Summary
        print("\n" + "="*60)
        print("OVERALL SUMMARY")
        print("="*60)
        
        total_expected = 0
        total_estimated = 0
        
        for result in results:
            total_expected += result['expected_products']
            total_estimated += result['estimated_links']
            print(f"{result['category']:25} | Expected: {result['expected_products']:4d} | Estimated: {result['estimated_links']:4d} | Diff: {result['difference']:+4d}")
        
        print("-" * 60)
        print(f"{'TOTAL':25} | Expected: {total_expected:4d} | Estimated: {total_estimated:4d} | Diff: {total_estimated - total_expected:+4d}")
        
        if total_expected > 0:
            accuracy = (total_estimated / total_expected) * 100
            print(f"\nLink detection accuracy: {accuracy:.1f}%")

async def main():
    tester = LinkCountTester()
    await tester.run_test()

if __name__ == "__main__":
    asyncio.run(main())