#!/usr/bin/env python3
"""
Simple test script to check connection to Farmaponte website
"""
import asyncio
import aiohttp
import ssl
from bs4 import BeautifulSoup

async def test_connection():
    """Test basic connection to Farmaponte"""
    
    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Headers to mimic a real browser
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
        
        # Test URLs
        test_urls = [
            "https://www.farmaponte.com.br/",
            "https://www.farmaponte.com.br/mamae-e-bebe/p=1?/",
            "https://www.farmaponte.com.br/mamae-e-bebe/"
        ]
        
        for url in test_urls:
            print(f"\n🔍 Testing connection to: {url}")
            
            try:
                async with session.get(url) as response:
                    print(f"✅ Status: {response.status}")
                    
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'lxml')
                        
                        # Check if we can find expected elements
                        title = soup.find('title')
                        if title:
                            print(f"📄 Page title: {title.get_text(strip=True)[:100]}...")
                        
                        # Look for product links on category pages
                        if "/p=" in url or "mamae-e-bebe" in url:
                            item_images = soup.find_all(class_="item-image")
                            print(f"🛍️  Found {len(item_images)} item-image elements")
                            
                            if item_images:
                                # Show first few product links found
                                for i, item in enumerate(item_images[:3]):
                                    link_tag = item.find('a')
                                    if link_tag and link_tag.get('href'):
                                        print(f"   Product {i+1}: {link_tag.get('href')}")
                        
                        print(f"📊 HTML length: {len(html)} characters")
                    else:
                        print(f"❌ HTTP {response.status}: {response.reason}")
                        
            except Exception as e:
                print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    print("🌐 Testing Farmaponte website connection...")
    asyncio.run(test_connection())
    print("\n✅ Connection test completed!")