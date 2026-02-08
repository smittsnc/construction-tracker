// Calculate date range for last 6 months
const today = new Date();
const sixMonthsAgo = new Date(today.getFullYear(), today.getMonth() - 6, today.getDate());
const dateRange = `from ${sixMonthsAgo.toLocaleDateString('en-US', { year: 'numeric', month: 'long' })} to ${today.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}`;

// Then use it in the extraction:
const articles = await stagehand.extract({
  instruction: `Extract all construction project announcement article titles and links ${dateRange}`,
  schema: {
    articles: [{
      title: "string",
      url: "string",
      date: "string"
    }]
  }
});
Here's the complete updated scraper.js:

import { Stagehand } from "@browserbasehq/stagehand";
import { createObjectCsvWriter } from "csv-writer";
import fs from "fs";

const websites = [
  { name: "NC", url: "https://ednc.com/news/" },
  { name: "SC", url: "https://www.sccommerce.com/news/" },
  { name: "GA", url: "https://georgia.org/press-releases" },
  { name: "TN", url: "https://tennesseelookout.com/category/working-the-economy/" },
  { name: "AR", url: "https://www.arkansasedc.com/news-events/newsroom" },
  { name: "AL", url: "https://www.madeinalbama.com/news/" },
  { name: "MS", url: "https://mississippi.org/news/" },
  { name: "LA", url: "https://www.opportunitylouisiana.gov/news" },
  { name: "CD", url: "https://www.constructiondive.com" }
];

async function scrapeWebsite(stagehand, site) {
  console.log(`\n🔍 Scraping ${site.name}...`);

  try {
    await stagehand.page.goto(site.url);
    await stagehand.page.waitForTimeout(3000);

    // Calculate date range for last 6 months
    const today = new Date();
    const sixMonthsAgo = new Date(today.getFullYear(), today.getMonth() - 6, today.getDate());
    const dateRange = `from ${sixMonthsAgo.toLocaleDateString('en-US', { year: 'numeric', month: 'long' })} to ${today.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}`;

    // Extract article links
    const articles = await stagehand.extract({
      instruction: `Extract all construction project announcement article titles and links ${dateRange}`,
      schema: {
        articles: [{
          title: "string",
          url: "string",
          date: "string"
        }]
      }
    });

    console.log(`✅ Found ${articles.articles?.length || 0} articles`);

    const projects = [];

    // Visit each article and extract project details
    for (const article of articles.articles || []) {
      try {
        console.log(`📄 Extracting: ${article.title}`);
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
        console.log(`⚠️ Error extracting ${article.title}: ${err.message}`);
      }
    }

    return projects;
  } catch (err) {
    console.log(`❌ Error scraping ${site.name}: ${err.message}`);
    return [];
  }
}

async function saveToCSV(projects) {
  const csvWriter = createObjectCsvWriter({
    path: "projects.csv",
    header: [
      { id: "projectName", title: "project_name" },
      { id: "customer", title: "customer" },
      { id: "generalContractor", title: "general_contractor" },
      { id: "announcementDate", title: "announcement_date" },
      { id: "projectValue", title: "project_value" },
      { id: "jobsCreated", title: "jobs_created" },
      { id: "city", title: "city" },
      { id: "county", title: "county" },
      { id: "state", title: "state" },
      { id: "articleUrl", title: "article_url" },
      { id: "source", title: "source" }
    ]
  });

  await csvWriter.writeRecords(projects);
  console.log(`\n✅ Saved ${projects.length} projects to projects.csv`);
}

async function main() {
  console.log("🚀 Starting Construction Project Scraper...");
  
  const stagehand = new Stagehand({
    apiKey: process.env.BROWSERBASE_API_KEY,
    verbose: true
  });

  await stagehand.init();

  let allProjects = [];

  for (const site of websites) {
    const projects = await scrapeWebsite(stagehand, site);
    allProjects = allProjects.concat(projects);
  }

  await saveToCSV(allProjects);
  await stagehand.close();

  console.log("\n✨ Scraper completed!");
}

main();
2. GitHub Actions Workflow (runs weekly)
Create a new file: .github/workflows/scraper.yml

name: Weekly Construction Scraper

on:
  schedule:
    - cron: '0 0 * * 0'  # Runs every Sunday at midnight UTC
  workflow_dispatch:     # Allows manual trigger from GitHub UI

jobs:
  scrape:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'
      
      - name: Install dependencies
        run: npm install
      
      - name: Run scraper
        env:
          BROWSERBASE_API_KEY: ${{ secrets.BROWSERBASE_API_KEY }}
        run: node scraper.js
      
      - name: Commit and push results
        run: |
          git config --local user.email "action@github.com"
          git config --local user.name "GitHub Action"
          git add projects.csv
          git commit -m "Update construction projects - $(date)" || echo "No changes to commit"
          git push