const mongoose = require('mongoose');
const axios = require('axios');
require('dotenv').config();

// MongoDB connection setup
const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log('Connected to MongoDB');
  } catch (error) {
    console.error('MongoDB connection error:', error.message);
    process.exit(1);
  }
};

// Define News Schema and Model
const newsSchema = new mongoose.Schema({
  title: String,
  description: String,
  link: String,
  pubDate: Date,
  source: String,
  tags: [String],
});
const News = mongoose.model('News', newsSchema);

// Sleep function to avoid API throttling
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Fetch news and store in MongoDB
const fetchAndStoreNews = async () => {
  const categories = ['sports', 'technology', 'business'];
  let totalCreditsUsed = 0;
  const maxCredits = 200;

  try {
    for (const category of categories) {
      if (totalCreditsUsed >= maxCredits) {
        console.log('Reached the maximum credit limit. Exiting.');
        break;
      }

      console.log(`Fetching latest articles from ${category}`);
      const response = await axios.get('https://newsdata.io/api/1/latest', {
        params: {
          apikey: process.env.API_KEY,
          category,
          country: 'in',
          language: 'en',
        },
      });

      const articles = response.data.results || [];
      if (articles.length === 0) {
        console.log(`No more articles available for ${category}.`);
        continue;
      }

      const mappedArticles = articles.map((article) => ({
        title: article.title || 'No Title',
        description: article.description || 'No Description',
        link: article.link || '',
        pubDate: new Date(article.pubDate) || new Date(),
        source: article.source_id || 'Unknown',
        tags: [category],
      }));

      await News.insertMany(mappedArticles, { ordered: false });
      console.log(`Inserted ${mappedArticles.length} articles from ${category}`);

      totalCreditsUsed += 1;
      console.log(`Total credits used: ${totalCreditsUsed}`);

      console.log('Waiting 30 seconds before the next request...');
      await sleep(30000);
    }

    console.log('News fetching job completed.');
  } catch (error) {
    console.error('Error fetching and storing news:', error.message);
  }
};

// Connect to MongoDB and run the fetch logic
(async () => {
  await connectDB();
  await fetchAndStoreNews();
})();
