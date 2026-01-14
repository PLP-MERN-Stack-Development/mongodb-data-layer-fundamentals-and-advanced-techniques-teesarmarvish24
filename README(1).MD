# PLP Bookstore - MongoDB Assignment (Week 1)

This repository contains the Node.js scripts for the Week 1 assignment on Data Layer Fundamentals.

## 📂 Files

1.  **insert_books.js**: Connects to MongoDB and populates the `books` collection with sample data.
2.  **queries.js**: Connects to MongoDB and performs CRUD operations, advanced queries, aggregation pipelines, and indexing tasks.

## 🛠️ Prerequisites

Before running the scripts, ensure you have the following installed:

- **Node.js**: [Download here](https://nodejs.org/)
- **MongoDB**: Ensure MongoDB is running locally (default port `27017`) or have your Atlas connection string ready.

## 🚀 How to Run

### 1. Initialize Project & Install Dependencies

Open your terminal in this project folder and run the following commands to create the `package.json` file and install the required MongoDB driver:

```bash
npm init -y
npm install mongodb
```

## 2. Setup Database

Run the setup script to create the database and insert sample data:

```bash

node insert_books.js
```

# Expected Output: Database initialized! Inserted 15 books.

## 3. Run Queries

Run the query script to execute the assignment tasks (CRUD, Aggregations, Indexing):

```bash

node queries.js
```

Expected Output: You will see console logs for Basic CRUD, Advanced Queries, Aggregations, and Indexing performance stats.

⚙️ Configuration
By default, the scripts connect to a local instance: const url = 'mongodb://localhost:27017';

If you are using MongoDB Atlas, update the url variable in both .js files with your actual connection string: const url = 'mongodb+srv://<username>:<password>@cluster.mongodb.net/';

📊 Tools Used
Node.js

MongoDB Driver for Node.js

Visual Studio Code
