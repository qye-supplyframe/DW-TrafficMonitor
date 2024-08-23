# Tracking Data Traffic Monitor

## Overview

**Tracking Data Traffic Monitor** is a tool designed to monitor trackiing data and to send alert emails when significant drops are observed. This tool helps in keeping track of traffic patterns and promptly responding to anomalies that might indicate issues or opportunities.

## Features

- **Hourly Data Monitoring**: Monitors orthogonal hourly traffic data to identify missiing directories or minimum traffic.
  - **Granularity**: country, (country, website), (country, website, action), (country, website, action, zone)
- **Daily Data Analysis**: Monitors eCommerceLayer daily traffic data for significant drops in traffic.
  - **Granularity**: server
- **Alert System**: Sends alert emails when significant drops in traffic are detected.

## Getting Started

### Prerequisites

- Scala
- Apache Spark

### Installation

1. **Clone the Repository**

```bash
git clone https://github.com/qye-supplyframe/DW-TrafficMonitor.git
cd TrackingDataTrafficMonitor
```

2. **Upload files to ym-02 Server**

```bash
 bash scripts/upload.sh
```

3. **Run Job on ym-02 Server**

```bash
chmod +x job.sh
./job.sh
```
