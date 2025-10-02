#!/usr/bin/env python3
"""
Debug script to examine HTML structure and find the correct selectors
"""
import asyncio
import aiohttp
import ssl
from bs4 import BeautifulSoup

async def debug_html_structure():
    """Debug the HTML structure of Farmaponte pages"""
    
    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    async with aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
        connector=connector
    ) as session:
        
        # Test the category page
        url = "https://www.farmaponte.com.br/mamae-e-bebe/p=1?/"
        print(f"🔍 Debugging URL: {url}")
        
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    print(f"✅ Successfully fetched page (Length: {len(html)})")
                    
                    # Save HTML for manual inspection
                    with open('debug_page.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                    print("💾 Saved HTML to debug_page.html")
                    
                    # Try different selectors for product links
                    selectors_to_try = [
                        "item-image",
                        ".item-image", 
                        "a[href*='/p']",
                        "a[href*='produto']",
                        ".product",
                        ".produto",
                        "[class*='product']",
                        "[class*='item']",
                        "a[href*='farmaponte.com.br']",
                        "img[alt*='produto']",
                        ".shelf-item",
                        ".vitrine-item"
                    ]
                    
                    print(f"\n🔍 Testing different selectors:")
                    for selector in selectors_to_try:
                        try:
                            if selector.startswith('.') or selector.startswith('['):
                                # CSS selector
                                elements = soup.select(selector)
                            else:
                                # Class name
                                elements = soup.find_all(class_=selector)
                            
                            print(f"   {selector}: {len(elements)} elements")
                            
                            if elements and len(elements) > 0:
                                # Show first few links found
                                for i, elem in enumerate(elements[:3]):
                                    if elem.name == 'a' and elem.get('href'):
                                        print(f"      Link {i+1}: {elem.get('href')}")
                                    elif elem.find('a'):
                                        link = elem.find('a')
                                        if link.get('href'):
                                            print(f"      Link {i+1}: {link.get('href')}")
                                            
                        except Exception as e:
                            print(f"   {selector}: Error - {str(e)}")
                    
                    # Look for any links with 'p' in them (product pages)
                    print(f"\n🔍 All links containing '/p':")
                    all_links = soup.find_all('a', href=True)
                    product_links = [link for link in all_links if '/p' in link.get('href', '')]
                    
                    for i, link in enumerate(product_links[:10]):  # Show first 10
                        print(f"   {i+1}: {link.get('href')}")
                    
                    print(f"\n📊 Total links with '/p': {len(product_links)}")
                    
                    # Look for common e-commerce patterns
                    print(f"\n🔍 Common e-commerce element counts:")
                    patterns = {
                        'Links total': len(soup.find_all('a', href=True)),
                        'Images': len(soup.find_all('img')),
                        'Classes with "product"': len(soup.find_all(class_=lambda x: x and 'product' in ' '.join(x).lower())),
                        'Classes with "item"': len(soup.find_all(class_=lambda x: x and 'item' in ' '.join(x).lower())),
                        'Classes with "shelf"': len(soup.find_all(class_=lambda x: x and 'shelf' in ' '.join(x).lower())),
                    }
                    
                    for pattern, count in patterns.items():
                        print(f"   {pattern}: {count}")
                        
                else:
                    print(f"❌ HTTP {response.status}: {response.reason}")
                    
        except Exception as e:
            print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(debug_html_structure())