**# 🛒 E-Commerce Analytics System**

**## Advanced Database Design and Implementation**



**Author: Abakar Sidick Baba Abakar Ali**  

**Course: Advanced Database Design**  

**Date March 2026**



---



\## 📋 PROJECT OVERVIEW



This project implements a comprehensive e-commerce analytics platform using three complementary big data technologies:



| Technology | Type | Purpose |

|------------|------|---------|

| \*\*MongoDB\*\* | Document Database | Store users, products, transactions |

| \*\*HBase\*\* | Wide-Column Database | Store user sessions (time-series data) |

| \*\*Apache Spark\*\* | Distributed Processing | Perform complex analytics across all data |



\*\*Dataset Summary:\*\*

\- 👥 \*\*1,000 users\*\* with demographic data

\- 📦 \*\*500 products\*\* across 20 categories

\- 🖱️ \*\*5,000 browsing sessions\*\* with page views

\- 💳 \*\*538 transactions\*\* with multiple items



---



\## 🏗️ SYSTEM ARCHITECTURE

┌─────────────────────────────────────────────────────────────────┐

│ DATA SOURCES │

│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │

│ │ users.json │ │products.json│ │sessions.json│ │

│ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ │

└─────────┼─────────────────┼─────────────────┼────────────────────┘

│ │ │

▼ ▼ ▼

┌─────────────────────────────────────────────────────────────────┐

│ DATA STORAGE LAYER │

│ ┌─────────────────┐ ┌─────────────────┐ │

│ │ MONGODB │ │ HBASE │ │

│ │ (Document) │ │ (Wide-Column) │ │

│ ├─────────────────┤ ├─────────────────┤ │

│ │ • users │ │ • user\_sessions │ │

│ │ • products │ │ • product\_metrics│ │

│ │ • transactions │ └─────────────────┘ │

│ │ • categories │ │

│ └────────┬────────┘ │

└───────────┼──────────────────────────────────────────────────────┘

│

▼

┌─────────────────────────────────────────────────────────────────┐

│ PROCESSING LAYER │

│ ┌───────────────────────┐ │

│ │ Apache Spark │ │

│ │ (Distributed SQL) │ │

│ └───────────┬───────────┘ │

└──────────────────────────┼───────────────────────────────────────┘

▼

┌─────────────────────────────────────────────────────────────────┐

│ VISUALIZATION LAYER │

│ ┌───────────────────────┐ │

│ │ Matplotlib/Seaborn │ │

│ │ 7 Professional Charts│ │

│ └───────────────────────┘ │

└─────────────────────────────────────────────────────────────────┘



---



\## 🍃 MONGODB IMPLEMENTATION



\### Collections Created

| Collection | Records | Key Feature |

|------------|---------|-------------|

| users | 1,000 | Embedded geo\_data (city, state) |

| products | 500 | category\_id reference |

| transactions | 538 | Embedded items array |

| categories | 20 | Hierarchical structure |



\### Sample Aggregation Query: Top Products by Revenue

```javascript

db.transactions.aggregate(\[

&nbsp; {$unwind: "$items"},

&nbsp; {$group: {

&nbsp;   \_id: "$items.product\_id",

&nbsp;   revenue: {$sum: "$items.subtotal"},

&nbsp;   orders: {$sum: 1}

&nbsp; }},

&nbsp; {$sort: {revenue: -1}},

&nbsp; {$limit: 10}

])

Result: Product prod\_00279 generated $6,199.97 from 8 orders



📊 HBASE IMPLEMENTATION

Table: user\_sessions

Row Key Design (CRITICAL!)

row\_key = user\_id + "\_" + reverse\_timestamp

where reverse\_timestamp = 99999999999999 - actual\_timestamp\_ms

Why this design?



user\_id prefix → all sessions for one user stored together



reverse\_timestamp → most recent sessions appear FIRST when scanning



Column Families

Family	Columns

session:	id, device, referrer, start\_time, end\_time, status

page\_views:	view\_0, view\_1, view\_2... (page type + duration)

cart:	product\_123 (quantity:price), total

Sample Query

\# Get all sessions for user\_000001

scanner = table.scan(row\_prefix=b'user\_000001')

⚡ SPARK ANALYTICS

Key Queries and Results

1\. Customer Lifetime Value (Top Customer)

esult: user\_000652 from Georgia spent $8,590 from just 4 orders!



2\. Conversion Funnel



Stage	Users	Percentage

Viewed Product	4,125	82.5%

Added to Cart	1,925	38.5%

Purchased	1,524	30.48%

3\. Monthly Revenue Trend



Month	Revenue

November 2025	$30,864

December 2025	$128,466 (PEAK!)

January 2026	$127,151

February 2026	$73,214

📈 VISUALIZATIONS (7 Charts)

Chart	Key Insight

revenue\_by\_state.png	Texas = $47,863 (top state)

monthly\_revenue.png	December = $128,466 (peak month)

conversion\_funnel.png	30.48% conversion rate

top\_products.png	prod\_00279 = $6,200

category\_pie.png	Category 6 = 15.2% of revenue

customer\_segments.png	47.8% one-time buyers

avg\_order\_value.png	Pennsylvania = $847 AOV (highest)

All charts are in the /charts folder.



💡 KEY BUSINESS INSIGHTS

Insight	Finding	Recommended Action

Top Market	Texas: $47,863 revenue	Increase marketing spend

Highest AOV	Pennsylvania: $847	Premium product testing

Peak Season	December: $128,466	Build inventory Oct-Nov

Conversion Rate	30.48% (EXCELLENT!)	Maintain funnel health

Top Product	prod\_00279: $6,200	Always keep in stock

Customer Loyalty	47.8% one-time buyers	Launch loyalty program

🚀 HOW TO RUN THIS PROJECT

Prerequisites



\# Install required libraries

pip install pyspark faker pandas pymongo happybase matplotlib

Step 1: Generate Dataset

python dataset\_generator\_fixed.py

Step 2: Start MongoDB and Load Data

\# Start MongoDB (as Administrator)

net start MongoDB

\# OR start manually:

"C:\\Program Files\\MongoDB\\Server\\8.2\\bin\\mongod.exe" --dbpath C:\\data\\db



\# Load data into MongoDB

python mongodb\_load\_fixed.py

Step 3: Start HBase (Docker)

docker run -d --name hbase-docker -p 9090:9090 dajobe/hbase

python hbase\_implementation.py

Step 4: Run Spark Analytics

python spark\_analytics\_fixed.py

Step 5: Generate Visualizations



python visualizations.py

📁 REPOSITORY STRUCTURE

├── 📁 charts/                 # 7 visualization PNG files

├── 📄 dataset\_generator\_fixed.py

├── 📄 mongodb\_load\_fixed.py

├── 📄 hbase\_implementation.py

├── 📄 spark\_analytics\_fixed.py

├── 📄 visualizations.py

├── 📄 FINAL EXAM REPORT.docx   # Exam answers

├── 📄 categories.json

├── 📄 products.json

├── 📄 users.json

├── 📄 sessions.json

├── 📄 transactions.json

└── 📄 README.md                # This file



KEY RESULTS SUMMARY



Metric	Value

Total Revenue	$359,695

Conversion Rate	30.48%

Top State Revenue	Texas = $47,863

Highest AOV	Pennsylvania = $847

Peak Month	December = $128,466

Top Product Revenue	prod\_00279 = $6,200

Best Customer	user\_000652 = $8,590





