import { LLMClient, Stagehand } from "@browserbasehq/stagehand";
import { createObjectCsvWriter } from "csv-writer";
import fs from "fs";
import { z } from "zod";

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
  await stagehand.page.goto(site.url, { waitUntil: 'domcontentloaded', timeout: 30000 });
} catch (navError) {
  console.log(`⚠️  Navigation error for ${site.url}, trying with networkidle...`);
  try {
    await stagehand.page.goto(site.url, { waitUntil: 'networkidle', timeout: 30000 });
  } catch (retryError) {
    console.log(`⚠️  Retry failed, continuing anyway...`);
  }
}
    await stagehand.page.waitForTimeout(3000);

    // Calculate date range for last 6 months
    const today = new Date();
    const sixMonthsAgo = new Date(today.getFullYear(), today.getMonth() - 6, today.getDate());
    const dateRange = `from ${sixMonthsAgo.toLocaleDateString('en-US', { year: 'numeric', month: 'long' })} to ${today.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}`;

    // Extract article links - FIXED: Use proper Zod schema
    const articles = await stagehand.page.extract({
      instruction: `Extract all construction project announcement article titles and links ${dateRange}`,
  schema: z.object({
    articles: z.array(z.object({
      title: z.string(),
      url: z.string(),
      date: z.string()
  }))
})
    });

    console.log(`✅ Found ${articles?.length || 0} articles`);

    const projects = [];

    // Visit each article and extract project details
    for (const article of articles?.articles || []) {
      try {
        // FIXED: Validate URL exists before navigating
        if (!article.url) {
          console.log(`⚠️ Skipping article with no URL: ${article.title}`);
          continue;
        }

        console.log(`📄 Extracting: ${article.title}`);
        await stagehand.page.goto(article.url);
        await stagehand.page.waitForTimeout(2000);

        // FIXED: Use proper Zod schema
        const projectData = await stagehand.page.extract({
          instruction: "Extract construction project details",
          schema: z.object({
            projectName: z.string().optional(),
            customer: z.string().optional(),
            generalContractor: z.string().optional(),
            announcementDate: z.string().optional(),
            projectValue: z.string().optional(),
            jobsCreated: z.string().optional(),
            city: z.string().optional(),
            county: z.string().optional(),
            state: z.string().optional()
          })
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
}


async function saveToCSV(projects) {
  // FIXED: Check if projects array is empty
if (!projects || projects.length === 0) {
  console.log('\n🚫 No projects to save, creating empty CSV');
  // Create empty CSV with headers only
  const csvWriter = createObjectCsvWriter({
    path: 'projects.csv',
    header: [
      { id: 'projectName', title: 'project_name' },
      { id: 'customer', title: 'customer' },
      { id: 'generalContractor', title: 'general_contractor' },
      { id: 'announcementDate', title: 'announcement_date' },
      { id: 'projectValue', title: 'project_value' },
      { id: 'jobsCreated', title: 'jobs_created' },
      { id: 'city', title: 'city' },
      { id: 'county', title: 'county' },
      { id: 'state', title: 'state' },
      { id: 'articleUrl', title: 'article_url' },
      { id: 'source', title: 'source' }
    ]
  });
  await csvWriter.writeRecords([]);
  return;
}

  try {
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
  } catch (err) {
    console.error(`❌ Error saving to CSV: ${err.message}`);
  }
}

async function main() {
  console.log("🚀 Starting Construction Project Scraper...");
  
  // FIXED: Add error handling for initialization
  let stagehand;
  try {
    stagehand = new Stagehand({
      apiKey: process.env.BROWSERBASE_API_KEY,
      LLMClient: {
        apiKey: process.env.OPENAI_API_KEY,
    model: "gpt-4o-mini"
      },
      verbose: true,
      headless: true
    });

    await stagehand.init();
  } catch (err) {
    console.error(`❌ Failed to initialize Stagehand: ${err.message}`);
    process.exit(1);
  }

  let allProjects = [];

  try {
    for (const site of websites) {
      const projects = await scrapeWebsite(stagehand, site);
      allProjects = allProjects.concat(projects);
    }

    await saveToCSV(allProjects);
  } finally {
    await stagehand.close();
  }

  console.log("\n✨ Scraper completed!");
}

main().catch(err => {
  console.error(`❌ Fatal error: ${err.message}`);
  process.exit(1);
});