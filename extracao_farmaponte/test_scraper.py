#!/usr/bin/env python3
"""
Test script with limited pages to verify the scraper works
"""
import asyncio
import sys
import os

# Add the current directory to Python path to import our scraper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extracao_farmaponte import FarmaponteScraper

async def test_limited_scraping():
    """Test scraping with limited pages and categories"""
    print("🧪 Testing limited scraping...")
    
    async with FarmaponteScraper(
        max_concurrent_requests=5,  # Lower concurrency for testing
        delay_between_requests=1.0   # Slower for testing
    ) as scraper:
        
        # Override categories with limited pages for testing
        scraper.categorias = {
            "mamae-e-bebe": "2",  # Only 2 pages for testing
            "suplementos": "1"    # Only 1 page for testing
        }
        
        try:
            # Step 1: Test category scraping
            print("\n🔍 Testing category page scraping...")
            category_results = await scraper.scrape_all_categories()
            
            # Show results
            total_links = 0
            for category, links in category_results.items():
                print(f"   {category}: {len(links)} products")
                total_links += len(links)
            
            print(f"📊 Total product links found: {total_links}")
            
            if total_links > 0:
                # Step 2: Test product detail scraping (limit to first 5 products)
                print("\n🔍 Testing product detail scraping...")
                all_links = []
                for links in category_results.values():
                    all_links.extend(list(links))
                
                # Limit to first 5 products for testing
                test_links = all_links[:5]
                print(f"   Testing with {len(test_links)} products")
                
                products_data = await scraper.scrape_product_details(test_links)
                
                # Show sample results
                for i, product in enumerate(products_data[:3]):
                    print(f"\n📦 Product {i+1}:")
                    for key, value in product.items():
                        if value:  # Only show non-empty values
                            print(f"   {key}: {value}")
                
                # Step 3: Test saving data
                print("\n💾 Testing data saving...")
                json_file = scraper.save_to_json(products_data, "test_products.json")
                csv_file = scraper.save_to_csv(products_data, "test_products.csv")
                
                print(f"✅ Test completed successfully!")
                print(f"   JSON: {json_file}")
                print(f"   CSV: {csv_file}")
            else:
                print("⚠️ No product links found - check the scraping logic")
                
        except Exception as e:
            print(f"❌ Test failed: {str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(test_limited_scraping())