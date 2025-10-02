#!/usr/bin/env python3
"""
Production script to run the full Farmaponte scraper
"""
import asyncio
import sys
import os
from datetime import datetime

# Add the current directory to Python path to import our scraper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extracao_farmaponte import FarmaponteScraper

async def run_full_scraper():
    """Run the full scraper on all categories"""
    print("🚀 Starting FULL Farmaponte scraping process...")
    print("⚠️  This will scrape ALL categories and may take a while!")
    
    # Ask for confirmation
    response = input("\nDo you want to continue? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("❌ Scraping cancelled by user")
        return
    
    start_time = datetime.now()
    
    async with FarmaponteScraper(
        max_concurrent_requests=10,  # Reasonable concurrency
        delay_between_requests=0.5   # Be respectful to the server
    ) as scraper:
        
        try:
            # Step 1: Scrape all categories
            print("\n🔍 STEP 1: Collecting product links from all categories")
            print("=" * 60)
            category_results = await scraper.scrape_all_categories()
            
            # Calculate totals
            total_links = sum(len(links) for links in category_results.values())
            print(f"\n📊 Total unique products found: {total_links}")
            
            if total_links == 0:
                print("❌ No products found. Exiting.")
                return
            
            # Save category links as backup
            links_file = scraper.save_category_links(category_results)
            
            # Step 2: Scrape product details
            print("\n🔍 STEP 2: Scraping detailed product information")
            print("=" * 60)
            
            # Combine all product links
            all_product_urls = set()
            for category, links in category_results.items():
                all_product_urls.update(links)
            
            product_urls_list = list(all_product_urls)
            print(f"📋 Processing {len(product_urls_list)} unique products...")
            
            # Optional: Ask if user wants to limit the number for testing
            limit_response = input("\nDo you want to limit the number of products for testing? (y/N): ").strip().lower()
            if limit_response in ['y', 'yes']:
                try:
                    limit = int(input("Enter the number of products to scrape (e.g., 100): "))
                    product_urls_list = product_urls_list[:limit]
                    print(f"📋 Limited to first {len(product_urls_list)} products")
                except ValueError:
                    print("❌ Invalid number, proceeding with all products")
            
            products_data = await scraper.scrape_product_details(product_urls_list)
            
            # Step 3: Save results
            print("\n💾 STEP 3: Saving scraped data")
            print("=" * 60)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file = scraper.save_to_json(products_data, f"farmaponte_products_full_{timestamp}.json")
            csv_file = scraper.save_to_csv(products_data, f"farmaponte_products_full_{timestamp}.csv")
            
            # Step 4: Summary
            print("\n📊 SCRAPING SUMMARY")
            print("=" * 60)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
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
            
            # Show category breakdown
            print(f"\n📋 Products by category:")
            for category, links in category_results.items():
                print(f"   {category}: {len(links)} products")
            
            print("\n🎉 Full scraping completed successfully!")
            
        except KeyboardInterrupt:
            print("\n🛑 Scraping interrupted by user")
        except Exception as e:
            print(f"\n❌ An error occurred during scraping: {str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(run_full_scraper())