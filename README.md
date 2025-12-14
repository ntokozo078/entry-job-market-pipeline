# 🇿🇦 South African Job Market Pipeline & Tracker

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Flask](https://img.shields.io/badge/Flask-Web%20App-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blueviolet)
![Status](https://img.shields.io/badge/Status-Live-success)

A full-stack data engineering project that automates the collection, cleaning, and visualization of entry-level tech jobs in South Africa and remote Data Engineering roles worldwide.

**[View Live Demo](https://sa-job-tracker.onrender.com)** *(Replace with your actual Render URL)*

---

## 📖 Project Overview

Finding entry-level tech jobs is fragmented. Aggregators often list expired roles ("zombie jobs"), and filtering for true "0-2 years experience" is difficult.

This project solves that by building a **daily automated pipeline** that:
1.  **Extracts** data from APIs (Adzuna) and web scrapers (Careers24).
2.  **Cleans & Normalizes** the data (removes 3+ year roles, filters expired dates).
3.  **Deduplicates** listings to prevent spam.
4.  **Stores** history in a PostgreSQL database (SCD Type 1).
5.  **Visualizes** market trends via a Flask web dashboard.

---

## 🏗️ Architecture


*Note: The pipeline runs on a daily schedule via Render Cron Jobs.*

**The Flow:**
1.  **Ingestion Layer:** Python scripts hit the Adzuna API and scrape target websites.
2.  **Processing Layer:** * **"Zombie Filter":** Rejects jobs with old years (e.g., 2019, 2021) in the title.
    * **"Seniority Gatekeeper":** Filters out Senior/Lead roles using keyword analysis.
    * **Deduplication:** Checks `source_job_id` to prevent duplicate entries.
3.  **Storage Layer:** Data is upserted into **PostgreSQL** (Hosted on Render).
4.  **Presentation Layer:** A **Flask** web app serves the data via a REST API and an HTML frontend with **Chart.js** analytics.

---

## 🛠️ Tech Stack

* **Language:** Python 3.10+
* **Web Framework:** Flask (Blueprints for modular architecture)
* **Database:** PostgreSQL (Production), SQLite (Local Dev)
* **ORM:** SQLAlchemy
* **ETL Tools:** `requests`, `BeautifulSoup4`
* **Frontend:** Bootstrap 5, Chart.js, Jinja2
* **Deployment:** Render (Web Service + Cron Job)

---

## 🚀 Key Features

* **Hybrid Ingestion:** Combines structured API data with unstructured scraped HTML.
* **Smart Filtering:** * Auto-detects "Remote" roles for Global Data Engineering jobs.
    * Strict "Entry Level" keyword enforcement.
* **Data Persistence:** Uses PostgreSQL to survive app restarts (unlike ephemeral file systems).
* **Interactive Dashboard:** Visualizes top skills (SQL, Python, AWS) and hiring locations.
* **REST API:** Exposes clean JSON data at `/api/jobs` for external consumption.

---


