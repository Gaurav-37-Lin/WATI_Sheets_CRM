<!-- README_Part1.md -->
# RentMAX WhatsApp Log Processor & Zoho CRM Integration – Technical Documentation

**Version:** 1.0  
**Date:** March 2025  
**Author:** RentMAX Development Team

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture and Workflow](#system-architecture-and-workflow)
3. [File Structure and Responsibilities](#file-structure-and-responsibilities)
    - [app.py](#apppy)
    - [rentmax_analysis.py](#rentmax_analysispy)
    - [requirements.txt](#requirementstxt)
4. [Environment Setup and Deployment](#environment-setup-and-deployment)
    - [Deploying on Render.com](#deploying-on-rendercom)
    - [Environment Variables](#environment-variables)
5. [WATI Webhook Integration](#wati-webhook-integration)
    - [Webhook Endpoint (/wati-webhook)](#webhook-endpoint-wati-webhook)
    - [Log File Storage and Format](#log-file-storage-and-format)

---

## 1. Project Overview

The **RentMAX WhatsApp Log Processor & Zoho CRM Integration** project automates the ingestion, processing, and synchronization of WhatsApp chat logs (from WATI) into Zoho CRM. The system:
- Receives messages via a secure webhook.
- Persists the messages in log files.
- Processes logs to extract structured tenant journey data.
- Updates existing leads in Zoho CRM based on matching Mobile and Lead_Source ("WATI") without creating duplicates.
- Uses OAuth 2.0 for secure API communication.
- Runs scheduled jobs via APScheduler.

---

## 2. System Architecture and Workflow

### 2.1 High-Level Architecture

The system consists of three primary layers:
1. **Data Ingestion:** A Flask app (`app.py`) handles incoming webhooks and OAuth callbacks.
2. **Data Processing:** A scheduled job reads log files and extracts journey data using logic in `rentmax_analysis.py`.
3. **Data Integration:** Processed data is pushed to Google Sheets (for backup) and updates existing Zoho CRM leads.

### 2.2 Data Flow Diagram (Mermaid)

```mermaid
flowchart TD
    subgraph WATI
        A[WATI Service]
    end
    subgraph FlaskApp
        B[Flask App (app.py)]
    end
    subgraph LogStorage
        C[Log Files]
    end
    subgraph Processor
        D[Journey Extraction (rentmax_analysis.py)]
    end
    subgraph External
        E[Google Sheets]
        F[Zoho CRM]
    end

    A -->|Webhook POST| B
    B -->|Append log entry| C
    B -->|Trigger Scheduled Job| D
    D --> E
    D --> F
