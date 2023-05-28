# Purchase-Analysis

**Business Scenario:** The grocery chain store is seeking an ETL developer to spearhead the creation of an efficient ETL pipeline that will facilitate the seamless migration of purchase data from the Online Transaction Processing (OLTP) system to the data warehouse. Additionally, the developer will be responsible for automating the process to ensure the smooth and timely loading of subsequent purchase data.

**Project Implemetation:**

**Step 1---> Restore the existing backup database:** To initiate the project, my first step was to restore the database from the backup and establish a solid foundation for the subsequent stages. This involved carefully executing the database restoration procedure, ensuring the successful recovery of the database to its original state.

Once the database was restored, I proceeded to create the necessary conceptual, logical, and physical relational models that aligned with the specific requirements outlined in the project specification. This comprehensive modeling process aimed to accurately represent the structure and relationships within the database.

The conceptual data model provided a high-level representation of the database, illustrating the key entities and their relationships. This model served as a blueprint for the subsequent stages of the project and provided a clear understanding of the overall database structure.

Moving forward, I delved into the logical data model, which focused on defining the entities, attributes, and relationships in a more detailed and abstract manner. This model emphasized the business logic and the logical organization of the data, ensuring that it aligned with the requirements and objectives of the project.

With the logical data model as a foundation, I transitioned into the physical data model, which involved translating the logical representation into the specific database management system (DBMS) implementation. This included defining tables, columns, data types, constraints, and indexes, ensuring optimal performance and efficient storage of the data.

Throughout the modeling process, I followed industry best practices and adhered to data modeling standards to guarantee consistency, maintainability, and scalability of the database. By meticulously crafting the conceptual, logical, and physical models, I established a solid groundwork that would guide the subsequent stages of the project, facilitating the successful implementation of the ETL pipeline and data migration process. Figure 1 shows the conceptual model.

 ![image](https://github.com/okwoli200/Purchase-Analysis/assets/99350558/fd18f666-9ebd-4dfe-bec3-9d170e07f765)
 **Figure 1: Conceptual Data Model**
 
 
 
**Step 2---> Create the ETL pipelines:** Using the dimensional modeling technique pioneered by Ralph Kimball, I designed a Data Warehouse by denormalizing the Online Transaction Processing (OLTP) system into a star schema. The star schema consisted of a central fact table called the purchase fact, surrounded by dimension tables. You can refer to Figure 2 for an example of this star schema, which illustrates the structure and relationships among the fact table and dimensions.

To accomplish this, I followed four key steps in the design process:

    Selection of the business process: I carefully identified the specific business process within the grocery chain store that required data analysis and reporting. This ensured that the Data Warehouse accurately represented the relevant operations and activities.

    Declaration of the grain: I defined the level of detail at which the data would be captured and stored in the Data Warehouse. This step established the appropriate granularity necessary to meet the reporting and analytical needs of the stakeholders.

    Identification of dimensions: I determined the dimensions that provided additional context and perspectives to the facts within the Data Warehouse. These dimensions included attributes and characteristics related to time, product, customer, and location, enabling multidimensional analysis and insights from various viewpoints.

    Identification of facts: I identified the measurable data points, represented by the purchase fact table, which formed the foundation for analysis and decision-making. This fact table contained relevant information such as product details, quantities, prices, and timestamps.

Subsequently, I wrote optimized SQL queries to extract data from the OLTP system, perform necessary transformations, and load the data into the staging environment. To facilitate the transformation and loading processes, I leveraged the .NET tools within SQL Server Integration Services (SSIS) to create pipelines.

To ensure data integrity throughout the ETL process, I implemented various tracking mechanisms. These included data pre-count, destination count, current count, Type 1 count (representing updates), Type 2 count (representing inserts), and post count. These counts served to monitor and track the movement of data, mitigating the risk of data loss and ensuring the accuracy of the final results.

Additionally, I established a separate pipeline dedicated to loading and truncating data from the staging environment into the Data Warehouse. This streamlined the transfer of data, maintaining consistency and integrity within the Data Warehouse.

In summary, my approach involved utilizing dimensional modeling techniques, optimizing SQL queries, and leveraging the .NET tools in SSIS to design and implement a robust Data Warehouse solution. The inclusion of tracking mechanisms and the use of dedicated pipelines ensured reliable data movement and integrity. This laid the groundwork for comprehensive data analysis and empowered informed decision-making within the grocery chain store.

![image](https://github.com/okwoli200/Purchase-Analysis/assets/99350558/2b652d19-f488-4bf6-beb2-20357d764230)
**Figure 2: Star Schema**

**Step 3---> Deploy the model:** I developed a control framework to govern the migration of data from the source to the staging environment and to the Data Warehouse. This framework encompassed key elements such as the target environment (staging or Data Warehouse), the frequency of data migration (daily, weekly, monthly, or yearly), package types (Dimensions or Facts), and the container for data transfer, known as a package.

To effectively measure and track the volume of data being moved, I incorporated metrics within the control framework. These metrics provided insights into the quantity and patterns of data migration, capturing information on a daily, weekly, monthly, and yearly basis.

To automate and streamline the data migration process according to the defined control framework, I leveraged the SQL Server Agent. This agent played a crucial role in scheduling and maintaining the execution of ETL tasks. By utilizing the capabilities of the SQL Server Agent, I ensured timely and efficient data migration while minimizing the need for manual intervention.

In summary, the control framework I implemented specified the target environment, migration frequency, package types, and incorporated metrics to track data movement. By deploying this framework using the SQL Server Agent, I automated the data migration process, enabling seamless and efficient updates to the Data Warehouse.



