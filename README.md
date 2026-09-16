# BookStore Co. Operational Platform

Deployment repository for the **BookStore Co. operational platform** used in the
**SI3008 - Data Intensive Systems** course.

This repository contains the files required to deploy the application in AWS.

The application source code is not included in this repository. The frontend and
backend are distributed as versioned Docker images through GitHub Container Registry (GHCR).

## Repository Structure

```text
.
├── README.md
├── .env.example
├── docker-compose.yml
├── database/
│   ├── schema.sql
│   ├── seed.sql
│   └── rds/
│       ├── bootstrap.env.example
│       ├── bootstrap.sh
│       ├── create_roles.sql
│       ├── grants.sql
│       └── verify.sql
└── docs/
```

## Requirements

- Docker
- Docker Compose
- An EC2 instance
- An Amazon RDS PostgreSQL instance
- Network connectivity between EC2 and RDS

## Configuration

Create the local environment file:

```bash
cp .env.example .env
```

Edit `.env` and configure the values corresponding to your AWS environment.

The RDS endpoint and database credentials are provided at runtime through this file.

The `.env` file must not be committed to the repository.

## Database Initialization

The `database/` directory contains the resources required to initialize the
BookStore operational database.

Follow the instructions provided in the laboratory guide to initialize the
database in Amazon RDS.

## Deploying the Application

Download the frontend and backend images:

```bash
sudo docker compose pull
```

Start the application:

```bash
sudo docker compose up -d
```

Verify the containers:

```bash
sudo docker compose ps
```

View the logs:

```bash
sudo docker compose logs -f
```

Stop the application:

```bash
sudo docker compose down
```

## Application Architecture

```text
Web Browser
    |
    v
Frontend
React + Nginx
    |
    v
Backend API
Node.js + Express
    |
    v
Amazon RDS for PostgreSQL
```

## Data 

Data could be download from:

Structured data: Data store in the postgres database.

https://si7002-2026-2-453927748990-us-east-1-an.s3.us-east-1.amazonaws.com/structured-data/bookstore_history_data.dump


## Course Use

This repository supports the BookStore Co. case used throughout the
Data Intensive Systems course.

Specific activities, architecture decisions, and deliverables are defined in
the corresponding laboratory guides.
