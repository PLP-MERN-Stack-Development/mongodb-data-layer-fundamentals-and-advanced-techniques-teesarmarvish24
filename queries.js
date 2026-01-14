/* Filename: queries.js
   Usage: node queries.js
*/

const { MongoClient } = require('mongodb');

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);
const dbName = 'plp_bookstore';

async function main() {
  try {
    await client.connect();
    console.log('Connected to server for queries...\n');

    const db = client.db(dbName);
    const collection = db.collection('books');

    
    // TASK 2: BASIC CRUD OPERATIONS    
    console.log("--- TASK 2: BASIC CRUD ---");

    // 1. Find all books in a specific genre
    console.log("1. Fantasy Books:");
    const fantasyBooks = await collection.find({ genre: "Fantasy" }).toArray();
    console.log(fantasyBooks);

    // 2. Find books published after a certain year
    console.log("\n2. Books published after 2000:");
    const modernBooks = await collection.find({ published_year: { $gt: 2000 } }).toArray();
    console.log(modernBooks);

    // 3. Find books by a specific author
    console.log("\n3. Books by Suzanne Collins:");
    const collinsBooks = await collection.find({ author: "Suzanne Collins" }).toArray();
    console.log(collinsBooks);

    // 4. Update the price of a specific book
    console.log("\n4. Updating price of '1984'...");
    await collection.updateOne(
      { title: "1984" },
      { $set: { price: 9.99 } }
    );
    console.log("Update complete.");

    // 5. Delete a book by its title
    console.log("\n5. Deleting 'The Hobbit'...");
    await collection.deleteOne({ title: "The Hobbit" });
    console.log("Delete complete.");


    
    // TASK 3: ADVANCED QUERIES    
    console.log("\n--- TASK 3: ADVANCED QUERIES ---");

    // 1. In stock and published after 2010
    console.log("1. In stock and published after 2010:");
    const stock2010 = await collection.find({ 
        in_stock: true, 
        published_year: { $gt: 2010 } 
    }).toArray();
    console.log(stock2010);

    // 2. Projection
    console.log("\n2. Projection (Title, Author, Price only):");
    const projection = await collection.find(
        {}, 
        { projection: { title: 1, author: 1, price: 1, _id: 0 } } // Note: 'projection' key is required in Node driver
    ).toArray();
    console.log(projection);

    // 3. Sorting
    console.log("\n3. Sorted by Price (Ascending):");
    const sorted = await collection.find().sort({ price: 1 }).toArray();
    console.log(sorted);

    // 4. Pagination
    console.log("\n4. Pagination (Page 1 - Top 5 books):");
    const page1 = await collection.find().limit(5).skip(0).toArray();
    console.log(page1);



    // TASK 4: AGGREGATION PIPELINE    
    console.log("\n--- TASK 4: AGGREGATION ---");

    // 1. Average price by genre
    console.log("1. Average price by Genre:");
    const avgPrice = await collection.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" }
        }
      }
    ]).toArray();
    console.log(avgPrice);

    // 2. Author with most books
    console.log("\n2. Author with the most books:");
    const topAuthor = await collection.aggregate([
      {
        $group: {
          _id: "$author",
          bookCount: { $sum: 1 }
        }
      },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log(topAuthor);

    // 3. Group by Decade
    console.log("\n3. Books grouped by Decade:");
    const decades = await collection.aggregate([
      {
        $project: {
          title: 1,
          published_year: 1,
          decade: { 
            $subtract: [ "$published_year", { $mod: ["$published_year", 10] } ] 
          }
        }
      },
      {
        $group: {
          _id: "$decade",
          count: { $sum: 1 },
          books: { $push: "$title" }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log(decades);


    
    // TASK 5: INDEXING    
    console.log("\n--- TASK 5: INDEXING ---");

    // 1. Create index
    console.log("Creating index on 'title'...");
    await collection.createIndex({ title: 1 });

    // 2. Create compound index
    console.log("Creating compound index on 'author' and 'published_year'...");
    await collection.createIndex({ author: 1, published_year: -1 });

    // 3. Explain
    console.log("\nPerformance Explain (Execution Stats):");
    // Note: explain() works on the cursor object in Node driver
    const explanation = await collection.find({ author: "Suzanne Collins" }).explain("executionStats");
    
    console.log("Execution Time (ms): " + explanation.executionStats.executionTimeMillis);
    console.log("Total Keys Examined: " + explanation.executionStats.totalKeysExamined);
    console.log("Total Docs Examined: " + explanation.executionStats.totalDocsExamined);

  } catch (err) {
    console.error("An error occurred:", err);
  } finally {
    await client.close();
  }
}

main();