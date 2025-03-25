# 💄 Makeup Shop – Distributed SQL Transaction Simulation

Multi-language SQL transaction simulator with GUI, concurrency control, and distributed database syncing.

---

## 📌 Overview

**Makeup Shop** is a hybrid application designed to demonstrate transactional anomalies and concurrency control across distributed databases using two languages and environments.

- 🧠 **Java GUI + CRUD**: Complete desktop app built in Java with full CRUD operations and user interface.
- 🐍 **Python Simulation**: Concurrent transactions and anomaly simulations (e.g., dirty reads, deadlocks, lost updates).
- 🐳 **Docker + Azure**: Two databases (MySQL and PostgreSQL) deployed via Docker and Azure, fully synchronized.
- 🔁 **Distributed Updates**: The system keeps both databases consistent by updating one based on changes detected in the other.

---

## ⚙️ Key Technologies

- **Java (IntelliJ)** – Swing GUI, JDBC, MySQL/PostgreSQL integration
- **Python (PyCharm)** – Multi-threaded simulation of isolation level anomalies
- **Docker** – Two containers for isolated DB environments
- **Azure Databases** – Cloud-hosted PostgreSQL and MySQL instances

---

## ▶️ Getting Started

1. Deploy Docker containers and connect to Azure-hosted databases.
2. Run Java app in IntelliJ for GUI and CRUD functionalities.
3. Run Python scripts for transactional tests and distributed sync.

---

