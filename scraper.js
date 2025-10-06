import { Stagehand } from "@browserbasehq/stagehand";
import { createObjectCsvWriter } from 'csv-writer';
import fs from 'fs';

const websites = [
  { name: "NC", url: "https://edpnc.com/news/" },
  { name: "SC", url: "https://www.sccommerce.com/news" },
  { name: "GA", url: "https://georgia.org/press-releases" },
  { name: "TN", url: "https://tennesseelookout.com/category/working-the-economy/" },
  { name: "AR", url: "https://www.arkansasedc.com/news-events/newsroom" },
  { name: "AL", url: "https://www.madeinalabama.com/news/" },
  { name: "MS", url: "https://mississippi.org/news/" },
  { name: "LA", url: "https://www.opportunitylouisiana.gov/news" },
  { name: "CD", url: "https://www.constructiondive.com" }
];

async function scrapeWebsite(stagehand, site) {
  console.log(`\n🔍 Scraping ${site.name}...`);
  
  try {
    await stagehand.page.goto(site.url);
    await stagehand.page.waitForTimeout(3000);
    
    // Extract article links
    const articles = await stagehand.extract({
      instruction: "Extract all construction project announcement article titles and links from 2025",
      schema: {
        articles: [{
          title: "string",
          url: "string",
          date: "string"
        }]
      }
    });
    
    console.log(`Found ${articles.articles?.length || 0} articles`);
    
    const projects = [];
    
    // Visit each article and extract project details
    for (const article of articles.articles || []) {
      try {
        console.log(`  📄 Extracting: ${article.title}`);
        await stagehand.page.goto(article.url);
        await stagehand.page.waitForTimeout(2000);
        
        const projectData = await stagehand.extract({
          instruction: "Extract construction project details",
          schema: {
            projectName: "string",
            customer: "string",
            generalContractor: "string",
            announcementDate: "string",
            projectValue: "string",
            jobsCreated: "string",
            city: "string",
            county: "string",
            state: "string"
          }
        });
        
        projects.push({
          ...projectData,
          articleUrl: article.url,
          source: site.name
        });
        
      } catch (err) {
        console.log(`  ⚠️  Error extracting ${article.title}: ${err.message}`);
      }
    }
    
    return projects;
    
  } catch (error) {
    console.error(`❌ Error scraping ${site.name}:`, error.message);
    return [];
  }
}

async function main() {
  const stagehand = new Stagehand({
    env: "BROWSERBASE",
    verbose: 1,
    debugDom: true
  });
  
  await stagehand.init();
  
  let allProjects = [];
  
  // Scrape each website
  for (const site of websites) {
    const projects = await scrapeWebsite(stagehand, site);
    allProjects = allProjects.concat(projects);
    
    // Save progress after each site
    saveToCSV(allProjects, 'projects_progress.csv');
  }
  
  // Final save
  saveToCSV(allProjects, 'projects_final.csv');
  
  console.log(`\n✅ Complete! Extracted ${allProjects.length} projects`);
  
  await stagehand.close();
}

function saveToCSV(projects, filename) {
  const csvWriter = createObjectCsvWriter({
    path: filename,
    header: [
      { id: 'projectName', title: 'Project Name' },
      { id: 'customer', title: 'Customer' },
      { id: 'generalContractor', title: 'General Contractor' },
      { id: 'announcementDate', title: 'Announcement Date' },
      { id: 'projectValue', title: 'Project Value' },
      { id: 'jobsCreated', title: 'Jobs Created' },
      { id: 'city', title: 'City' },
      { id: 'county', title: 'County' },
      { id: 'state', title: 'State' },
      { id: 'articleUrl', title: 'Article URL' },
      { id: 'source', title: 'Source' }
    ]
  });
  
  csvWriter.writeRecords(projects)
    .then(() => console.log(`💾 Saved to ${filename}`));
}

main();