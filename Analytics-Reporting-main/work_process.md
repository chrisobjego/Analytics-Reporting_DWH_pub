###  Project Objectives

Since the foundation of the company, we relied on external tools for data-analytic activities (Googe Analytics, Mixpanel, etc.). This was fine for the beginning but may become a potential risk/dependency going forward since we are always tied to the specific providers and the availability of their service.
Moreover, we are more and more facing restrictions when we want to analyze the user-journey end-to-end where we nowadays have different tools in place (GA for the top of the funnel; Mixpanel for Product)
Mid-to long-term we also might have to dig deeper into analyzing the data our users are capturing in company to provide additional services on top (e.g. energy consumption benchmarking)

Additionally,

•	We get different results per metric per data source.


•	The data in the reporting dashboard are not fully automated.


•	Manual and laborious analytic process for the data team

Based on this, we have the following objectives


•	Improve Data Quality: By extracting data from various sources and transforming it into a consistent format, the data quality will be improved, resulting in more accurate analysis and decision-making.


•	Enable Data-Driven Decisions: With a centralized data warehouse, the organization can make data-driven decisions based on real-time information, which can lead to improved business outcomes.


•	Increase Efficiency: By automating the ETL process, data can be extracted and loaded into the data warehouse more quickly and efficiently, reducing the need for manual data entry.


•	Enhance Data Security: By centralizing data into a single, secure data warehouse, the organization can ensure that sensitive data is protected and accessed only by authorized personnel.


•	Improve Data Accessibility: With a centralized data warehouse, data can be accessed more easily and quickly, enabling faster and more accurate decision-making.


•	Streamline Reporting: By having a centralized data warehouse, the organization can streamline reporting processes, reducing the time and effort required to generate reports.



## Specific use-cases include



Manual data manipulation: Most times analyzing data that are on cross platforms, for example, breaking down churned users per channel involves downloading data from Chargebee and Mixpanel/GA into an Excel sheet. 


another example would be analyzing the portfolio size of paying user involves accessing data from the database for the portfolio, downloading data from Chargebee for the paying, and combing in an Excel sheet. 


Data Integration: As at the time of this documentation, there is a need to intelligently get cohorts created in mixpanel into the Hubspot for targetted messaging.



End-to-end Analysis: Analysing user journeys from website visits to product usage and churn currently requires manual effort which is time-consuming and less efficient.



## Scope and Deliverables:


Project Scope: All current data sources will be integrated into the data warehouse


Deliverables: A centralized data warehouse


Assumptions: All our data sources can integrate with the chosen data warehouse


Constraints: Budget, timeline, capacity

