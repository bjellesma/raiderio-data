# Raiderio Data

Raiderio Data is designed to ingest the [raiderio api](https://raider.io/api) in order to analyze the mythic plus scores over the years for [World of Warcraft](https://worldofwarcraft.blizzard.com/) for the top percentages of players. The data is ingested from an AWS lambda function and transformed into an ETL (extract, transform, and load) process through AWS glue workflow that is then able to be visualized through [Grafana](https://grafana.com/).

This project was built as part of [David Freitag's Build Your First Serverless Data Engineering Project](https://maven.com/david-freitag/first-serverless-de-project)

# Questions to Answer

What has been and what is the mythic plus score need to be in top 40% of players? Top 25%? Top 10%? Top 1%? Top 0.1%? This is according to when World of Warcraft's mythic plus scores were first tracked in raider.io

# Architecture

![Architecture](images/architecture.png)